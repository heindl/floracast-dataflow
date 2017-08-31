from __future__ import absolute_import
from apache_beam.options.pipeline_options import PipelineOptions
import logging
from tensorflow.core.example import example_pb2
from google.cloud.proto.datastore.v1 import entity_pb2
import apache_beam as beam
from elevation import ElevationBundleDoFn
from weather import FetchWeatherDoFn
from encode import EncodeExampleDoFn
# pip install "apache_beam[gcp]"

class OccurrencePipelineOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):

        parser.add_argument(
            '--output',
            required=True,
            help='Output file to write results to.')

        # parser.add_argument(
        #     '--streaming',
        #     required=False,
        #     default=False,
        #     help='Whether streaming mode is enabled or disabled; true if enabled.')

        # parser.add_argument(
        #     '--runner',
        #     required=False,
        #     default="DataflowRunner",
        #     help='DataflowRunner or DirectRunner')

        # parser.add_argument(
        #     '--project',
        #     required=True,
        #     help='Your project ID is required in order to run your pipeline on the Google Cloud Dataflow Service.')

        # parser.add_argument(
        #     '--template_location',
        #     required=False,
        #     help='Location of template file')

        # parser.add_argument(
        #     '--staging_location',
        #     required=False,
        #     default="gs://occurrence_dataflow/staging/",
        #     help='Your Google Cloud Storage path is required for staging local files.')

        # parser.add_argument(
        #     '--temp_location',
        #     required=False,
        #     default="gs://occurrence_dataflow/temp/",
        #     help='Your Google Cloud Storage path is required for temporary files.')

        # parser.add_argument(
        #     '--job_name',
        #     required=True,
        #     help='Name of the dataflow job.')

        parser.add_argument(
            '--minimum_occurrences_within_taxon',
            required=False,
            default=40,
            help='The number of occurrence required to process taxon')

        parser.add_argument(
            '--taxon',
            required=True,
            help='Restrict occurrence fetch to this taxa')

        parser.add_argument(
            '--weather_station_distance',
            required=False,
            default=75,
            help='Maximum distance a weather station can be from an occurrence when fetching weather.')


# Filter and prepare for duplicate sort.
@beam.typehints.with_input_types(entity_pb2.Entity)
@beam.typehints.with_output_types(str)
class OccurrenceEntityToString(beam.DoFn):

    def __init__(self, taxon=0):
        from apache_beam.metrics import Metrics
        super(OccurrenceEntityToString, self).__init__()
        self.new_occurrence_counter = Metrics.counter('main', 'new_occurrences')
        self.invalid_occurrence_date = Metrics.counter('main', 'invalid_occurrence_date')
        self.invalid_occurrence_elevation = Metrics.counter('main', 'invalid_occurrence_elevation')
        self.invalid_occurrence_location = Metrics.counter('main', 'invalid_occurrence_location')
        self._taxon = int(taxon)

    def process(self, element):
        from google.cloud.datastore.helpers import entity_from_protobuf, GeoPoint
        from google.cloud.datastore import Entity
        import logging
        """
            Element should be an occurrence entity.
            The key has should be a sufficient key.
        """
        e = entity_from_protobuf(element)

        if self._taxon != 0:
            if e.key.parent.parent.id != self._taxon:
                return

        self.new_occurrence_counter.inc()

        # This is a hack to avoid indexing the 'Date' property in Go.
        if e['Date'].year < 1970:
            self.invalid_occurrence_date.inc()
            return

        if 'Elevation' not in e:
            self.invalid_occurrence_elevation.inc()
            return

        loc = e['Location']
        elevation = e['Elevation']

        (lat, lng) = (0.0, 0.0)
        if type(loc) is GeoPoint:
            lat = loc.latitude
            lng = loc.longitude
        elif type(loc) is Entity:
            lat = loc['Lat']
            lng = loc['Lng']
        else:
            logging.error("invalid type: %s", type(loc))
            return

        if lng > -52.2330:
            logging.info("%.6f && %.6f", lat, lng)
            self.invalid_occurrence_location.inc()
            return

        yield "%d|||%.8f|||%.8f|||%d|||%.8f" % (
            e.key.parent.parent.id,
            lat,
            lng,
            int(e['Date'].strftime("%s")),
            elevation
        )

@beam.typehints.with_input_types(example_pb2.SequenceExample)
@beam.typehints.with_output_types(example_pb2.SequenceExample)
class AddRandomTrainPoint(beam.DoFn):
    def __init__(self):
        super(AddRandomTrainPoint, self).__init__()
        self.NORTHERNMOST = 49.
        self.SOUTHERNMOST = 25.
        self.EASTERNMOST = -66.
        self.WESTERNMOST = -124.

    def random_point(self):
        import random
        from datetime import datetime

        date = datetime(
            random.randint(2015, 2017),
            random.randint(1, 12),
            random.randint(1, 28)
        )
        lat = round(random.uniform(self.SOUTHERNMOST, self.NORTHERNMOST), 6)
        lng = round(random.uniform(self.EASTERNMOST, self.WESTERNMOST), 6)
        return lat, lng, date
        # while True:
        #     lat = round(random.uniform(self.SOUTHERNMOST, self.NORTHERNMOST), 6)
        #     lng = round(random.uniform(self.EASTERNMOST, self.WESTERNMOST), 6)
        #     try:
        #         gcode = Geocoder.reverse_geocode(lat, lng)
        #
        #         if gcode[0].data[0]['formatted_address'][-6:] in ('Canada', 'Mexico'):
        #             continue
        #         elif 'unnamed road' in gcode[0].data[0]['formatted_address']:
        #             continue
        #         elif 'Unnamed Road' in gcode[0].data[0]['formatted_address']:
        #             continue
        #         else:
        #             return gcode[0].coordinates[0], gcode[0].coordinates[1], date
        #     except GeocoderError:
        #         continue

    def process(self, element):
        from tensorflow.core.example import example_pb2
        import mgrs
        yield element

        lat, lng, date = self.random_point()
        se = example_pb2.SequenceExample()
        se.context.feature["label"].int64_list.value.append(0)
        se.context.feature["latitude"].float_list.value.append(lat)
        se.context.feature["longitude"].float_list.value.append(lng)
        se.context.feature["date"].int64_list.value.append(int(date.strftime("%s")))
        se.context.feature["grid-zone"].bytes_list.value.append(mgrs.MGRS().toMGRS(lat, lng)[:2].encode())

        yield se


@beam.typehints.with_input_types(str)
@beam.typehints.with_output_types(beam.typehints.KV[int, example_pb2.SequenceExample])
class StringToTaxonSequenceExample(beam.DoFn):
    def __init__(self):
        super(StringToTaxonSequenceExample, self).__init__()
        from apache_beam.metrics import Metrics
        self.following_removed_duplicates = Metrics.counter('main', 'following_removed_duplicates')

    def process(self, element):
        import mgrs
        from tensorflow.core.example import example_pb2

        self.following_removed_duplicates.inc()

        ls = element.split("|||")
        taxon = int(ls[0])
        lat = float(ls[1])
        lng = float(ls[2])
        intDate = int(ls[3])
        elevation = float(ls[4])

        se = example_pb2.SequenceExample()
        se.context.feature["label"].int64_list.value.append(taxon)
        se.context.feature["latitude"].float_list.value.append(lat)
        se.context.feature["longitude"].float_list.value.append(lng)
        se.context.feature["date"].int64_list.value.append(intDate)
        se.context.feature["elevation"].float_list.value.append(elevation)
        se.context.feature["grid-zone"].bytes_list.value.append(mgrs.MGRS().toMGRS(lat, lng)[:2].encode())

        yield taxon, se


def datastore_query():
    from google.cloud.proto.datastore.v1 import query_pb2
    # from googledatastore import helper
    # from google.cloud.datastore import Filter
    # q = query.Query(kind='Occurrence', project=project)
    # q.fetch()
    q = query_pb2.Query()
    q.kind.add().name='Occurrence'
    # datastore_helper.set_kind(q, 'Occurrence')
    # q.limit.name = 100

    # Need to index this in google.

    # helper.set_property_filter(
    #     Filter(),
    #     '__key__', datastore.PropertyFilter.HAS_ANCESTOR,
    #     default_todo_list.key))

    # datastore_helper.set_composite_filter(q.filter, CompositeFilter.AND,
    # datastore_helper.set_property_filter(ds.Filter(), 'Location.Lat', PropertyFilter.GREATER_THAN_OR_EQUAL, 5.4995),
    # datastore_helper.set_property_filter(ds.Filter(), 'Location.Lat', PropertyFilter.LESS_THAN_OR_EQUAL, 83.1621),
    # datastore_helper.set_property_filter(ds.Filter(), 'Location.Lng', PropertyFilter.GREATER_THAN_OR_EQUAL, -167.2764),
    # https://stackoverflow.com/questions/41705870/geospatial-query-at-google-app-engine-datastore
    # The query index may not be implemented at this point.
    # datastore_helper.set_property_filter(datastore.Filter(), 'Location.longitude', PropertyFilter.LESS_THAN_OR_EQUAL, -52.2330)
    # )
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
    occurrence_pipeline_options = pipeline_options.view_as(OccurrencePipelineOptions)
    standard_options = pipeline_options.view_as(StandardOptions)

    def unwind_and_filter((key, values)):
        # This consumes all the data from shuffle
        value_list = list(values)
        if len(value_list) >= 50:
            yield value_list

    with beam.Pipeline(options=google_cloud_options) as p:

        (p
            | 'ReadDatastoreOccurrences' >> ReadFromDatastore(
                project=google_cloud_options.project,
                query=datastore_query()
            )
            | 'ConvertEntitiesToKeyStrings' >> beam.ParDo(OccurrenceEntityToString(occurrence_pipeline_options.taxon))
            | 'RemoveDuplicates' >> beam.RemoveDuplicates()
            | 'ConvertKeyStringsToTaxonSequenceExample' >> beam.ParDo(StringToTaxonSequenceExample())
            | 'GroupByTaxon' >> beam.GroupByKey()
            # | 'FilterTaxonWithInsufficientOccurrences' >> beam.Filter(
            #         lambda (taxon, occurrences): len(list(occurrences)) > occurrence_pipeline_options.minimum_occurrences_within_taxon
            #    )
            | 'FilterAndUnwindOccurrences' >> beam.FlatMap(lambda (taxon, occurrences):
                 occurrences if len(list(occurrences)) >= occurrence_pipeline_options.minimum_occurrences_within_taxon else [])
            | 'AddRandomTrainPoint' >> beam.ParDo(AddRandomTrainPoint())
            | 'EnsureElevation' >> beam.ParDo(ElevationBundleDoFn(google_cloud_options.project))
            | 'FetchWeather' >> beam.ParDo(FetchWeatherDoFn(
                    google_cloud_options.project,
                    occurrence_pipeline_options.weather_station_distance
                ))
            | 'EncodeForWrite' >> beam.ParDo(EncodeExampleDoFn())
            | 'Write' >> beam.io.WriteToTFRecord(
                    file_path_prefix=occurrence_pipeline_options.output,
                    file_name_suffix='.tfrecord',
                )
         )
        #
        # result = p.run()
        # result.wait_until_finish()
        # #
        # # # Print metrics if local runner.
        # if standard_options.runner == "DirectRunner":
        #
        #     for v in [
        #         'new_occurrences',
        #         'invalid_occurrence_elevation',
        #         'invalid_occurrence_location',
        #         'invalid_occurrence_date',
        #         'following_removed_duplicates',
        #         'sufficient_taxa',
        #         'insufficient_taxa',
        #         'insufficient_weather_records',
        #         'final_occurrence_count',
        #     ]:
        #         query_result = result.metrics().query(MetricsFilter().with_name(v))
        #         if query_result['counters']:
        #             logging.info('%s: %d', v, query_result['counters'][0].committed)


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()