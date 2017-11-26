# from __future__ import absolute_import
from __future__ import division

import logging
import apache_beam as beam
from apache_beam.io import iobase
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions, SetupOptions
import os
from fetch_shared import elevation, weather, utils, ex
from tensorflow_transform.beam import impl as tft
from datetime import datetime as dt
from apache_beam.io import range_trackers
from datetime import datetime
from geopy import Point
import time
from google.cloud.firestore_v1beta1 import client

# If error after upgradeing apache beam: metaclass conflict: the metaclass of a derived class must be a (non-strict) subclass of the metaclasses of all its bases
# then: pip install six==1.10.0

class LocalPipelineOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):

        #### FETCH ####

        # Intermediate TFRecords are stored in their own directory, each with a corresponding metadata file.
        # The metadata lists how many records, how many of each taxon label.
        parser.add_value_provider_argument(
            '--data_location',
            required=False,
            help='The intermediate TFRecords file that contains downloaded features from BigQuery'
        )

        parser.add_value_provider_argument(
            '--max_weather_station_distance',
            required=False,
            default=100,
            help='Maximum distance a weather station can be from an occurrence when fetching weather.')

        #### INFER ####

        parser.add_value_provider_argument(
            '--protected_area_count',
            required=False,
            default=0,
            type=int,
            help='The number of areas to fetch. Gathers all if zero.'
        )

        parser.add_value_provider_argument(
            '--date',
            required=False,
            type=str,
            help='The date on which to gather wilderness areas.'
        )


def run(argv=None):

    pipeline_options = PipelineOptions()

    local_pipeline_options = pipeline_options.view_as(LocalPipelineOptions)
    cloud_options = pipeline_options.view_as(GoogleCloudOptions)
    cloud_options.project = utils.default_project()
    standard_options = pipeline_options.view_as(StandardOptions)
    pipeline_options.view_as(SetupOptions).save_main_session = True

    # Having issues validating in live version.
    if local_pipeline_options.data_location is None:
        return
    if local_pipeline_options.date is None:
        return

    with beam.Pipeline(standard_options.runner, options=pipeline_options) as pipeline:
        with tft.Context(temp_dir=cloud_options.temp_location):

            output = os.path.join(
                str(local_pipeline_options.data_location),
                str(local_pipeline_options.date),
                dt.now().strftime("%s"),
            )

            _ = pipeline \
                | _ReadProtectedAreas(project=cloud_options.project, protected_area_count=local_pipeline_options.protected_area_count) \
                | 'ConvertProtectedAreaDictToExample' >> beam.ParDo(_ProtectedAreaDictToExample(local_pipeline_options.date)) \
                | 'GroupByYearMonthRegion' >> utils.GroupByYearMonthRegion() \
                | 'FetchWeather' >> beam.ParDo(weather.FetchWeatherDoFn(cloud_options.project, local_pipeline_options.max_weather_station_distance)) \
                | 'EnsureElevation' >> beam.ParDo(elevation.ElevationBundleDoFn(cloud_options.project)) \
                | 'ProtoForWrite' >> beam.Map(lambda e: e.encode()) \
                | 'WriteDataAsTFRecord' >> beam.io.WriteToTFRecord(output+"/areas", file_name_suffix='.tfrecord.gz')


# Filter and prepare for duplicate sort.
@beam.typehints.with_input_types(dict)
@beam.typehints.with_output_types(ex.Example)
class _ProtectedAreaDictToExample(beam.DoFn):

    def __init__(self, date_str):
        super(_ProtectedAreaDictToExample, self).__init__()
        self._date_str = date_str

    def process(self, element):
        # from google.cloud.datastore.helpers import entity_from_protobuf

        """
            Element should be an occurrence entity.
            The key has should be a sufficient key.
        """
        # e = entity_from_protobuf(element)
        # centre = self._parse_point(e['Centre'])
        centre = Point(element["Centre"]["Latitude"], element["Centre"]["Longitude"])
        e = ex.Example()
        e.set_occurrence_id("%.6f|%.6f|%s" % (centre.latitude, centre.longitude, self._date_str))
        e.set_longitude(centre.longitude)
        e.set_latitude(centre.latitude)
        e.set_date(int(time.mktime(datetime.strptime(self._date_str, "%Y%m%d").timetuple())))
        yield e

class _ProtectedAreaSource(iobase.BoundedSource):

    def __init__(self, project, protected_area_count):
        # from apache_beam.metrics import Metrics
        # self.records_read = Metrics.counter(self.__class__, 'recordsRead')
        self._project = project
        self._protected_area_count = protected_area_count

    def estimate_size(self):
        return 0

    def get_range_tracker(self, start_position, stop_position):

        if start_position is None:
            start_position = 0
        if stop_position is None:
            stop_position = range_trackers.OffsetRangeTracker.OFFSET_INFINITY

        # Use an unsplittable range tracker. This means that a collection can
        # only be read sequentially for now.
        range_tracker = range_trackers.OffsetRangeTracker(start_position, stop_position)
        range_tracker = range_trackers.UnsplittableRangeTracker(range_tracker)

        return range_tracker

    def read(self, range_tracker):

        db = client.Client(project=self._project)
        q = db.collection(u'WildernessAreas')
        if self._protected_area_count > 0:
            q = q.limit(self._protected_area_count)

        for w in q.get():
            yield w.to_dict()

    def split(self, desired_bundle_size, start_position=None, stop_position=None):
        """Implements :class:`~apache_beam.io.iobase.BoundedSource.split`
        This function will currently not be called, because the range tracker
        is unsplittable
        """

        if start_position is None:
            start_position = 0
        if stop_position is None:
            stop_position = range_trackers.OffsetRangeTracker.OFFSET_INFINITY

        # Because the source is unsplittable (for now), only a single source is
        # returned.
        yield iobase.SourceBundle(
            weight=1,
            source=self,
            start_position=start_position,
            stop_position=stop_position)


class _ReadProtectedAreas(beam.PTransform):
    """A :class:`~apache_beam.transforms.ptransform.PTransform` for reading
    from MongoDB.
    """
    def __init__(self, project, protected_area_count):
        """Initializes :class:`ReadFromMongo`
        Uses source :class:`_MongoSource`
        """
        super(_ReadProtectedAreas, self).__init__()
        self._source = _ProtectedAreaSource(project, protected_area_count)

    def expand(self, pcoll):
        """Implements :class:`~apache_beam.transforms.ptransform.PTransform.expand`"""
        return pcoll | iobase.Read(self._source)

        # def display_data(self):
        #     return {'source_dd': self._source}

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()