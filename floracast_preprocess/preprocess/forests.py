# from __future__ import absolute_import
import apache_beam as beam
from google.cloud.proto.datastore.v1 import entity_pb2

from example import Example


class SplitPCollsByDate(beam.DoFn):
    def __init__(self):
        super(SplitPCollsByDate, self).__init__()

    def process(self, example):
        from apache_beam import pvalue
        yield pvalue.TaggedOutput(
            example.date_string(), example)

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
            friday = today + timedelta((4 - today.weekday()) % 7)
            dates = []
            unix = []
            for d in date_range(end=friday, periods=options['weeks_before'], freq='W').tolist():
                dates.append(d.strftime("%y%m%d"))
                unix.append(int(d.strftime('%s')))

            examples = pipeline \
                  | _ReadDatastoreForests(project=options['project']) \
                  | 'ConvertForestEntityToExample' >> beam.ParDo(_ForestEntityToExample(unix)) \
                  | 'EnsureElevation' >> beam.ParDo(elevation.ElevationBundleDoFn(options['project'])) \
                  | 'FetchWeather' >> beam.ParDo(weather.FetchWeatherDoFn(options['project'], options['weather_station_distance'])) \
                  | 'SplitPCollsByDate' >> beam.ParDo(SplitPCollsByDate()).with_outputs(*dates)

            for d in dates:
                path = output_path+"/"+d+"/"
                _ = examples[d] \
                    | ("ProtoForWrite-%s" % d) >> beam.Map(lambda e: e.encode()) \
                    | ("WritePredictDataAsTFRecord-%s" % d) >> beam.io.WriteToTFRecord(path, file_name_suffix='.tfrecord.gz')

                _ = examples[d] \
                    | ("EncodePredictAsB64Json-%s" % d) >> beam.Map(utils.encode_as_b64_json) \
                    | ("WritePredictDataAsText-%s" % d) >> beam.io.WriteToText(path, file_name_suffix='.txt')


# Filter and prepare for duplicate sort.
@beam.typehints.with_input_types(entity_pb2.Entity)
@beam.typehints.with_output_types(Example)
class _ForestEntityToExample(beam.DoFn):

    def __init__(self, dates=None):
        super(_ForestEntityToExample, self).__init__()
        # dates are expected to be unix timestamp integers.
        self._dates = dates

    def _parse_point(self, loc):
        from geopy import Point
        from google.cloud.datastore.helpers import GeoPoint
        from google.cloud.datastore import Entity

        if type(loc) is GeoPoint:
            return Point(loc.latitude, loc.longitude)
        elif type(loc) is Entity:
            return Point(loc['Lat'], loc['Lng'])
        else:
            raise ValueError('Could not parse Geopoint')

    def process(self, element):
        from google.cloud.datastore.helpers import entity_from_protobuf
        from example import Example
        """
            Element should be an occurrence entity.
            The key has should be a sufficient key.
        """
        e = entity_from_protobuf(element)
        centre = self._parse_point(e['Centre'])
        for d in self._dates:
            ex = Example()
            ex.set_occurrence_id("%.6f|%.6f|%s" % (centre.latitude, centre.longitude, d.strftime("%y%m%d")))
            ex.set_longitude(centre.longitude)
            ex.set_latitude(centre.latitude)
            ex.set_date(d)
            yield ex


class _ReadDatastoreForests(beam.PTransform):
    """Wrapper for reading from either CSV files or from BigQuery."""

    def __init__(self, project):
        super(_ReadDatastoreForests, self).__init__()
        self._project = project

    def expand(self, pvalue):
        from apache_beam.io.gcp.datastore.v1.datastoreio import ReadFromDatastore
        from google.cloud.proto.datastore.v1 import query_pb2

        q = query_pb2.Query()
        q.kind.add().name='WildernessArea'

        return (pvalue.pipeline
                | 'ReadDatastoreForests' >> ReadFromDatastore(
            project=self._project,
            query=q
            )
        )