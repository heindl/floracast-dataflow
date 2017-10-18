# from __future__ import absolute_import
import apache_beam as beam
# from google.cloud.proto.datastore.v1 import entity_pb2
# from apache_beam.transforms.core import PTransform
from apache_beam.io import iobase

from example import Example

class SplitPCollsByDate(beam.DoFn):
    def __init__(self):
        super(SplitPCollsByDate, self).__init__()

    def process(self, example):
        from apache_beam import pvalue
        yield pvalue.TaggedOutput(
            example.date_string(), example)


@beam.ptransform_fn
def DiffuseByDate(pcoll):  # pylint: disable=invalid-name
    return (pcoll
            | 'ProjectDateToDefuse' >> beam.Map(lambda e: (e.date_string(), e))
            | 'GroupByKeyToDiffuse' >> beam.GroupByKey()
            | 'UngroupToDefuse' >> beam.FlatMap(lambda v: v[1]))


def fetch_forests(
        pipeline_options,
        output_path,
    ):
    import elevation as elevation
    import weather as weather
    from tensorflow_transform.beam import impl as tft
    from datetime import datetime, timedelta
    from pandas import date_range
    import utils

    options = pipeline_options.get_all_options()

    with beam.Pipeline(options['runner'], options=pipeline_options) as pipeline:
        with tft.Context(temp_dir=options['temp_location']):

            # Fetch dates
            today = datetime.now()
            # TODO: This isn't friday. Need to recalculate.
            friday = today + timedelta((4 - today.weekday()) % 7)
            dates = []
            unix = []
            for d in date_range(end=friday, periods=options['weeks_before'], freq='W').tolist():
                dates.append(d.strftime("%Y%m%d"))
                unix.append(int(d.strftime('%s')))

            examples = pipeline \
                  | _ReadProtectedAreas(project=options['project'], protected_area_count=options['protected_area_count']) \
                  | 'ConvertProtectedAreaDictToExample' >> beam.ParDo(_ProtectedAreaDictToExample(unix)) \
                  | 'EnsureElevation' >> beam.ParDo(elevation.ElevationBundleDoFn(options['project'])) \
                  | 'DiffuseByDate' >> DiffuseByDate() \
                  | "FetchWeather" >> beam.ParDo(weather.FetchWeatherDoFn(options['project'], options['weather_station_distance'])) \
                  | 'SplitPCollsByDate' >> beam.ParDo(SplitPCollsByDate()).with_outputs(*dates)

            for d in dates:
                path = output_path+"/"+d
                _ = examples[d] \
                    | ("ProtoForWrite-%s" % d) >> beam.Map(lambda e: e.encode()) \
                    | ("WritePredictDataAsTFRecord-%s" % d) >> beam.io.WriteToTFRecord(path, file_name_suffix='.tfrecord.gz')

                # _ = examples[d] \
                #     | ("EncodePredictAsB64Json-%s" % d) >> beam.Map(utils.encode_as_b64_json) \
                #     | ("WritePredictDataAsText-%s" % d) >> beam.io.WriteToText(path, file_name_suffix='.txt')


# Filter and prepare for duplicate sort.
@beam.typehints.with_input_types(dict)
@beam.typehints.with_output_types(Example)
class _ProtectedAreaDictToExample(beam.DoFn):

    def __init__(self, dates=None):
        super(_ProtectedAreaDictToExample, self).__init__()
        # dates are expected to be unix timestamp integers.
        self._dates = dates

    # def _parse_point(self, loc):
    #     from geopy import Point
    #     from google.cloud.datastore.helpers import GeoPoint
    #     from google.cloud.datastore import Entity
    #
    #     if type(loc) is GeoPoint:
    #         return Point(loc.latitude, loc.longitude)
    #     elif type(loc) is Entity:
    #         return Point(loc['Lat'], loc['Lng'])
    #     else:
    #         raise ValueError('Could not parse Geopoint')

    def process(self, element):
        # from google.cloud.datastore.helpers import entity_from_protobuf
        from example import Example
        from datetime import datetime
        from geopy import Point
        """
            Element should be an occurrence entity.
            The key has should be a sufficient key.
        """
        # e = entity_from_protobuf(element)
        # centre = self._parse_point(e['Centre'])
        centre = Point(element["Centre"]["Latitude"], element["Centre"]["Longitude"])
        for d in self._dates:
            ex = Example()
            ex.set_occurrence_id("%.6f|%.6f|%s" % (centre.latitude, centre.longitude, datetime.fromtimestamp(d).strftime("%Y%m%d")))
            ex.set_longitude(centre.longitude)
            ex.set_latitude(centre.latitude)
            ex.set_date(d)
            yield ex


class _ProtectedAreaSource(iobase.BoundedSource):

    def __init__(self, project, protected_area_count):
        # from apache_beam.metrics import Metrics
        # self.records_read = Metrics.counter(self.__class__, 'recordsRead')
        self._project = project
        self._protected_area_count = protected_area_count

    def estimate_size(self):
        return 0

    def get_range_tracker(self, start_position, stop_position):
        from apache_beam.io import range_trackers
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
        from google.cloud import firestore

        db = firestore.Client(project=self._project)
        q = db.collection(u'WildernessAreas')
        if self._protected_area_count > 0:
            q = q.limit(self._protected_area_count)

        print("Wilderness Area Query", q)
        for w in q.get():
            print("Have element", w)
            yield w.to_dict()

    def split(self, desired_bundle_size, start_position=None, stop_position=None):
        """Implements :class:`~apache_beam.io.iobase.BoundedSource.split`
        This function will currently not be called, because the range tracker
        is unsplittable
        """
        from apache_beam.io import range_trackers

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