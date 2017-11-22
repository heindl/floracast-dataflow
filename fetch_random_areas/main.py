# from __future__ import absolute_import
from __future__ import division

import logging
import apache_beam as beam
from apache_beam.io import iobase
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions, SetupOptions
import os
from shared import elevation, weather, utils
from tensorflow_transform.beam import impl as tft
from datetime import datetime as dt
from apache_beam.io import range_trackers
from shared import ex
from apache_beam.transforms.core import PTransform

# If error after upgradeing apache beam: metaclass conflict: the metaclass of a derived class must be a (non-strict) subclass of the metaclasses of all its bases
# then: pip install six==1.10.0

class LocalPipelineOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):

        #### FETCH ####

        # Intermediate TFRecords are stored in their own directory, each with a corresponding metadata file.
        # The metadata lists how many records, how many of each taxon label.
        parser.add_argument(
            '--output_location',
            required=True,
            help='The intermediate TFRecords file that contains downloaded features from BigQuery'
        )

        parser.add_argument(
            '--max_weather_station_distance',
            required=False,
            default=100,
            help='Maximum distance a weather station can be from an occurrence when fetching weather.')

        #### INFER ####

        parser.add_argument(
            '--random_area_count',
            required=True,
            default=0,
            type=int,
            help='The number of areas to fetch. Gathers all if zero.'
        )


def run(argv=None):

    pipeline_options = PipelineOptions()

    local_pipeline_options = pipeline_options.view_as(LocalPipelineOptions)
    cloud_options = pipeline_options.view_as(GoogleCloudOptions)
    standard_options = pipeline_options.view_as(StandardOptions)
    pipeline_options.view_as(SetupOptions).setup_file = os.path.abspath(os.path.join(os.path.dirname(__file__), 'setup.py'))
    pipeline_options.view_as(SetupOptions).save_main_session = True

    output_path = os.path.join(
        local_pipeline_options.output_location,
        dt.now().strftime("%s"),
    )

    if standard_options.runner == 'DirectRunner':
        os.makedirs(output_path)

    with beam.Pipeline(standard_options.runner, options=pipeline_options) as pipeline:
        with tft.Context(temp_dir=cloud_options.temp_location):

            occurrences = pipeline \
                          | 'ReadRandomOccurrences' >> _ReadRandomOccurrences(count=local_pipeline_options.random_area_count) \
                          | 'RemoveOccurrenceExampleLocationDuplicates' >> utils.RemoveOccurrenceExampleLocationDuplicates() \
                          | 'GroupByYearMonth' >> utils.GroupByYearMonth() \
                          | 'FetchWeather' >> beam.ParDo(weather.FetchWeatherDoFn(cloud_options.project, local_pipeline_options.max_weather_station_distance)) \
                          | 'EnsureElevation' >> beam.ParDo(elevation.ElevationBundleDoFn(cloud_options.project)) \
                          | 'ShuffleOccurrences' >> utils.Shuffle() \
                          | 'ProtoForWrite' >> beam.Map(lambda e: e.encode())

            _ = occurrences \
                | 'WriteRandomOccurrences' >> beam.io.WriteToTFRecord(output_path + "/", file_name_suffix='.tfrecord.gz')

class _RandomOccurrenceSource(iobase.BoundedSource):

    def __init__(self, count=0):
        self._count = count

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
        for _ in range(0, self._count):
            yield ex.RandomExample()

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

class _ReadRandomOccurrences(PTransform):
    """A :class:`~apache_beam.transforms.ptransform.PTransform` for reading
    from MongoDB.
    """
    def __init__(self, count):
        """Initializes :class:`ReadFromMongo`
        Uses source :class:`_MongoSource`
        """
        super(_ReadRandomOccurrences, self).__init__()
        self._source = _RandomOccurrenceSource(count=count)

    def expand(self, pcoll):
        """Implements :class:`~apache_beam.transforms.ptransform.PTransform.expand`"""
        return pcoll | iobase.Read(self._source)



if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()