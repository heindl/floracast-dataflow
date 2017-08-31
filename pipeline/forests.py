from __future__ import absolute_import
from apache_beam.options.pipeline_options import PipelineOptions
import logging
from tensorflow.core.example import example_pb2
from google.cloud.proto.datastore.v1 import entity_pb2
import apache_beam as beam
# pip install "apache_beam[gcp]"
from elevation import ElevationBundleDoFn
from weather import FetchWeatherDoFn
from encode import EncodeExampleDoFn

class ForestPipelineOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):

        parser.add_argument(
            '--output',
            required=True,
            help='Output file to write results to.')

        parser.add_argument(
            '--weeks_before',
            required=False,
            default=1,
            help='The number of weeks in the past to generate prediction data for each forest'
            # If the model changes, we can expect this to be 52 weeks in the past. If not, just this week,
            # calculated every Friday.
        )

        parser.add_argument(
            '--weather_station_distance',
            required=False,
            default=50,
            help='Maximum distance a weather station can be from an occurrence when fetching weather.')

# Filter and prepare for duplicate sort.
@beam.typehints.with_input_types(entity_pb2.Entity)
@beam.typehints.with_output_types(str)
class ForestEntityToSequenceExample(beam.DoFn):

    def __init__(self, friday=None, periods=1):
        from datetime import datetime
        super(ForestEntityToSequenceExample, self).__init__()
        self._friday = friday
        if friday is None:
            today = datetime.date.today()
            self._friday = today + datetime.timedelta((4 - today.weekday()) % 7)
        self._periods = periods

    # type Area struct {
    #     Key         *datastore.Key        `datastore:"__key__"`
    # State       string                `datastore:",omitempty"`
    # Acres       float64               `datastore:",omitempty"`
    # Name        string                `datastore:",omitempty"`
    # BoundingBox [2]datastore.GeoPoint `datastore:",omitempty"`
    # }

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
        from google.cloud.datastore.helpers import entity_from_protobuf, GeoPoint
        from geopy import Point, distance
        from google.cloud.datastore import Entity
        from pandas import date_range
        import mgrs
        from tensorflow.core.example import example_pb2
        import logging
        """
            Element should be an occurrence entity.
            The key has should be a sufficient key.
        """

        e = entity_from_protobuf(element)

        nw = self._parse_point(e['BoundingBox'][0])
        se = self._parse_point(e['BoundingBox'][1])

        # Calculate center of bounding box.
        center = distance.GreatCircleDistance(
            miles=distance.GreatCircleDistance(se, nw).miles
        ).destination(se, 315)

        for d in date_range(end=self._friday, periods=self._periods, freq='W').tolist():

            se = example_pb2.SequenceExample()
            se.context.feature["label"].int64_list.value.append(e.key.id)
            se.context.feature["latitude"].float_list.value.append(center.latitude)
            se.context.feature["longitude"].float_list.value.append(center.longitude)
            se.context.feature["date"].int64_list.value.append(int(d.strftime("%s")))
            se.context.feature["grid-zone"].bytes_list.value.append(mgrs.MGRS().toMGRS(center.latitude, center.longitude)[:2].encode())
            yield se


def forest_query():
    from google.cloud.proto.datastore.v1 import query_pb2
    q = query_pb2.Query()
    q.kind.add().name='WildernessArea'
    return q

def run():
    from apache_beam.io.gcp.datastore.v1.datastoreio import ReadFromDatastore
    from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions
    # from apache_beam.metrics import MetricsFilter
    # parser = argparse.ArgumentParser()

    # Set defaults
    pipeline_options = PipelineOptions(
        #['--taxon', 58583]
    )

    google_cloud_options = pipeline_options.view_as(GoogleCloudOptions)
    forest_pipeline_options = pipeline_options.view_as(ForestPipelineOptions)
    standard_options = pipeline_options.view_as(StandardOptions)

    with beam.Pipeline(options=google_cloud_options) as p:

        (p
         | 'ReadDatastoreOccurrences' >> ReadFromDatastore(
            project=google_cloud_options.project,
            query=forest_query()
        )
         | 'ConvertForestEntityToSequenceExample' >> beam.ParDo(ForestEntityToSequenceExample(forest_pipeline_options.weeks_before))
         | 'EnsureElevation' >> beam.ParDo(ElevationBundleDoFn(google_cloud_options.project))
         | 'FetchWeather' >> beam.ParDo(FetchWeatherDoFn(
                google_cloud_options.project,
                forest_pipeline_options.weather_station_distance
            ))
         | 'EncodeForWrite' >> beam.ParDo(EncodeExampleDoFn())
         | 'Write' >> beam.io.WriteToTFRecord(
            file_path_prefix=forest_pipeline_options.output,
            file_name_suffix='.tfrecord',
        )
         )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()