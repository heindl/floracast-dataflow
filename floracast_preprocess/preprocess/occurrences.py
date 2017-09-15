# from __future__ import absolute_import
import apache_beam as beam
from example import Example
from google.cloud.proto.datastore.v1 import entity_pb2


def fetch_occurrences(
        pipeline_options,
        output_path,
):
    import apache_beam as beam
    import weather as weather
    import example as example
    import elevation as elevation
    from apache_beam.io import WriteToText
    import utils as utils
    from tensorflow_transform.beam import impl as tft

    options = pipeline_options.get_all_options()

    with beam.Pipeline(options['runner'], options=pipeline_options) as pipeline:
        with tft.Context(temp_dir=options['temp_location']):

            data = pipeline \
                   | _ReadDatastoreOccurrences(project=options['project']) \
                   | 'ConvertEntitiesToExamples' >> beam.ParDo(_OccurrenceEntityToExample(options['occurrence_taxa'])) \
                   | 'RemoveOccurrenceExampleLocationDuplicates' >> _RemoveOccurrenceExampleLocationDuplicates() \
                   | 'RemoveScantTaxa' >> _RemoveScantTaxa(options['minimum_occurrences_within_taxon'])

            if options['add_random_train_point'] is True:
                data = data | 'AddRandomTrainPoint' >> beam.FlatMap(lambda e: [e, example.RandomExample()])

            data = data \
                   | 'EnsureElevation' >> beam.ParDo(elevation.ElevationBundleDoFn(options['project'])) \
                   | 'DefuseOne' >> beam.FlatMap(lambda e: [e]) \
                   | 'DefuseTwo' >> beam.FlatMap(lambda e: [e]) \
                   | 'FetchWeather' >> beam.ParDo(weather.FetchWeatherDoFn(options['project'], options['weather_station_distance'])) \
                   | 'ShuffleOccurrences' >> utils.Shuffle() \
                   | 'ProtoForWrite' >> beam.Map(lambda e: e.encode())

            # schema = example.make_input_schema(mode)
            # proto_coder = coders.ExampleProtoCoder(schema)
            #
            # data = data | 'EncodeForWrite' >> beam.Map(proto_coder.encode)

            _ = data \
                | 'Write' >> beam.io.WriteToTFRecord(output_path, file_name_suffix='.tfrecord.gz')

            # _ = data \
            #     | 'EncodePredictAsB64Json' >> beam.Map(utils.encode_as_b64_json) \
            #     | 'WritePredictDataAsText' >> beam.io.WriteToText(output_path, file_name_suffix='.txt')


            # Write metadata
            _ = beam.Create([{
                'taxa': options['occurrence_taxa'],
                'weather_station_distance': options['weather_station_distance'],
                'minimum_occurrences_within_taxon': options['minimum_occurrences_within_taxon'],
                'random_train_points': options['add_random_train_point']
            }]) \
                | 'WriteToMetadataFile' >> WriteToText(output_path+"/"+"metadata", file_name_suffix=".meta")

# Filter and prepare for duplicate sort.
@beam.typehints.with_input_types(entity_pb2.Entity)
@beam.typehints.with_output_types(Example)
class _OccurrenceEntityToExample(beam.DoFn):

    def __init__(self, taxon=0):
        from apache_beam.metrics import Metrics
        super(_OccurrenceEntityToExample, self).__init__()
        self.new_occurrence_counter = Metrics.counter('main', 'new_occurrences')
        self.invalid_occurrence_date = Metrics.counter('main', 'invalid_occurrence_date')
        self.invalid_occurrence_elevation = Metrics.counter('main', 'invalid_occurrence_elevation')
        self.invalid_occurrence_location = Metrics.counter('main', 'invalid_occurrence_location')
        self._taxa = [int(taxon)]

    def process(self, element):
        from google.cloud.datastore.helpers import entity_from_protobuf, GeoPoint
        from google.cloud.datastore import Entity
        import logging
        """
            Element should be an occurrence entity.
            The key has should be a sufficient key.
        """
        e = entity_from_protobuf(element)

        if self._taxa is not None:
            if e.key.parent.parent.id not in self._taxa:
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

        ex = Example()
        ex.set_date(int(e['Date'].strftime("%s")))
        ex.set_latitude(lat)
        ex.set_longitude(lng)
        ex.set_elevation(elevation)
        ex.set_taxon(e.key.parent.parent.id)
        ex.set_occurrence_id(str(e.key.id))

        yield ex


class Counter(beam.DoFn):
    def __init__(self, which):
        self._which = which
        self._counter = 0

    def process(self, element):
        self._counter += 1
        print(self._which, self._counter, element.occurrence_id())
        yield element


class _RemoveScantTaxa(beam.PTransform):
    """Count as a subclass of PTransform, with an apply method."""

    def __init__(self, minimum_occurrences_within_taxon):
        self._minimum_occurrences_within_taxon = minimum_occurrences_within_taxon

    def expand(self, pcoll):
        return pcoll \
                | 'ExamplesToTaxonTuples' >> beam.Map(lambda e: (e.taxon(), e)) \
                | 'GroupByTaxon' >> beam.GroupByKey() \
                | 'FilterAndUnwindOccurrences' >> beam.FlatMap(
            lambda (taxon, occurrences): occurrences if len(list(occurrences)) >= self._minimum_occurrences_within_taxon else [])


@beam.ptransform_fn
def _RemoveOccurrenceExampleLocationDuplicates(pcoll):  # pylint: disable=invalid-name
    """Produces a PCollection containing the unique elements of a PCollection."""
    return pcoll \
            | 'ToPairs' >> beam.Map(lambda e: (e.equality_key(), e)) \
            | 'GroupByKey' >> beam.GroupByKey() \
            | 'Combine' >> beam.Map(lambda (key, examples): list(examples)[0])


class _ReadDatastoreOccurrences(beam.PTransform):

    def __init__(self, project):
        super(_ReadDatastoreOccurrences, self).__init__()
        self._project = project

    def expand(self, pvalue):
        from apache_beam.io.gcp.datastore.v1.datastoreio import ReadFromDatastore
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

        return (pvalue.pipeline
                | 'ReadDatastoreOccurrences' >> ReadFromDatastore(
                        project=self._project,
                        query=q
                  )
                )