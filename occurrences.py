from __future__ import absolute_import

import argparse
import apache_beam as beam
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.io.gcp.datastore.v1.datastoreio import ReadFromDatastore
import logging
from astral import Astral
from google.cloud.proto.datastore.v1 import query_pb2
from google.cloud.datastore import helpers
from googledatastore import helper as datastore_helper, PropertyFilter, CompositeFilter
import googledatastore as ds
from apache_beam.metrics import Metrics
from apache_beam.metrics.metric import MetricsFilter
import tensorflow as tf

class Occurrence(object):
    def __init__(self, example):
        self.example = example

    def __eq__(self, other):

        s = self.example.features.feature
        n = other.example.features.feature

        if s['label'].int64_list.value[0] != n['label'].int64_list.value[0]:
            return False

        if s['date'].int64_list.value[0] != n['date'].int64_list.value[0]:
            return False

        if s['latitude'].int64_list.value[0] != n['latitude'].int64_list.value[0]:
            return False

        if s['longitude'].int64_list.value[0] != n['longitude'].int64_list.value[0]:
            return False

        return True


class OccurrenceCoder(beam.coders.Coder):

    def encode(self, o):
        return o.example.SerializeToString()

    def decode(self, o):
        features = tf.parse_single_example(
            o,
            features={
                'label': tf.FixedLenFeature([], tf.int64),
                'date': tf.FixedLenFeature([], tf.int64),
                'latitude': tf.FixedLenFeature([], tf.float64),
                'longitude': tf.FixedLenFeature([], tf.float64),
                'tmax': tf.FixedLenFeature([60], tf.float32),
                'tmin': tf.FixedLenFeature([60], tf.float32),
                'prcp': tf.FixedLenFeature([60], tf.float32),
                'daylength': tf.FixedLenFeature([], tf.int64),
                # 'elevation': tf.FixedLenFeature([], tf.int64),
            })
        return Occurrence(tf.train.Example(features=tf.train.Features(feature=features)))

    def is_deterministic(self):
        return True


beam.coders.registry.register_coder(Occurrence, OccurrenceCoder)


@beam.typehints.with_input_types(helpers.Entity)
@beam.typehints.with_output_types(Occurrence)
class EntityToOccurrence(beam.DoFn):
    def __init__(self):
        super(EntityToOccurrence, self).__init__()
        self.new_occurrence_counter = Metrics.counter('main', 'new_occurrences')
        self.invalid_occurrence_counter = Metrics.counter('main', 'invalid_occurrences')

    def process(self, element):
        """
            Element should be an occurrence entity.
            The key has should be a sufficient key.
        """
        e = helpers.entity_from_protobuf(element)

        # This is a hack to avoid indexing the 'Date' property in Go.
        if e['Date'].year < 1970:
            self.invalid_occurrence_counter.inc()
            return

        self.new_occurrence_counter.inc()

        loc = e['Location']

        a = Astral()
        a.solar_depression = 'civil'
        astro = a.sun_utc(e['Date'], loc['Lat'], loc['Lng'])
        daylength = (astro['sunset'] - astro['sunrise']).seconds

        yield Occurrence(tf.train.Example(features=tf.train.Features(feature={
            'label': tf.train.Feature(int64_list=tf.train.Int64List(value=[e.key.parent().parent().id()])),
            'latitude': tf.train.Feature(int64_list=tf.train.FloatList(value=[loc['Lat']])),
            'longitude': tf.train.Feature(int64_list=tf.train.FloatList(value=[loc['Lng']])),
            'date': tf.train.Feature(int64_list=tf.train.Int64List(value=[int(e['Date'].strftime("%s"))])),
            'daylength': tf.train.Feature(int64_list=tf.train.Int64List(value=[int(daylength)]))
        })))


@beam.typehints.with_input_types(Occurrence)
@beam.typehints.with_output_types(beam.typehints.KV[int, Occurrence])
class GroupTaxa(beam.DoFn):
    def __init__(self):
        super(GroupTaxa, self).__init__()

    def process(self, o):
        yield o.example.features.feature['label'].int64_list.value[0], o


@beam.typehints.with_input_types(beam.typehints.KV[int, beam.typehints.Iterable[Occurrence]])
@beam.typehints.with_output_types(Occurrence)
class UnwindTaxaWithSufficientCount(beam.DoFn):
    def __init__(self):
        super(UnwindTaxaWithSufficientCount, self).__init__()
        self.sufficient_taxa_counter = Metrics.counter('main', 'sufficient_taxa')
        self.insufficient_taxa_counter = Metrics.counter('main', 'insufficient_taxa')
        self.final_occurrence_count = Metrics.counter('main', 'final_occurrences')

    def process(self, element):
        if len(element[1]) <= 50:
            self.insufficient_taxa_counter.inc()
            return

        self.sufficient_taxa_counter.inc()

        for o in element[1]:
            self.final_occurrence_count.inc()
            yield o

# @beam.typehints.with_input_types(beam.typehints.KV[str, beam.typehints.Iterable[Occurrence]])
# @beam.typehints.with_output_types(beam.typehints.List(Occurrence))
# class ProjectSufficientOccurrencesDoFn(beam.DoFn):
#     def __init__(self):
#         super(ProjectSufficientOccurrencesDoFn, self).__init__()
#
#     def process(self, t):
#         """
#             Element should be an occurrence entity.
#             The key has should be a sufficient key.
#         """
#         if len(t[1]) <= 50:
#             return
#
#         for o in t[1]:
#             yield [o]

# class LogDoFn(beam.DoFn):
#     def __init__(self):
#         super(LogDoFn, self).__init__()
#
#     def process(self, o):
#         logging.info(o)
#         yield o

def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--output',
                        dest='output',
                        # CHANGE 1/5: The Google Cloud Storage path is required
                        # for outputting the results.
                        default='./output.csv',
                        help='Output file to write results to.')
    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_args.extend([
        # CHANGE 2/5: (OPTIONAL) Change this to DataflowRunner to
        # run your pipeline on the Google Cloud Dataflow Service.
        '--runner=DirectRunner',
        # CHANGE 3/5: Your project ID is required in order to run your pipeline on
        # the Google Cloud Dataflow Service.
        '--project=floracast-20c01',
        # CHANGE 4/5: Your Google Cloud Storage path is required for staging local
        # files.
        '--staging_location=gs://YOUR_BUCKET_NAME/AND_STAGING_DIRECTORY',
        # CHANGE 5/5: Your Google Cloud Storage path is required for temporary
        # files.
        '--temp_location=gs://YOUR_BUCKET_NAME/AND_TEMP_DIRECTORY',
        '--job_name=occurrence-fetcher',
    ])

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True

    p = beam.Pipeline(options=pipeline_options)

    project = pipeline_options.get_all_options()['project']

    q = query_pb2.Query()
    q.kind.add().name = 'Occurrence'
    # Need to index this in google.

    # datastore_helper.set_composite_filter(q.filter, CompositeFilter.AND,
        # datastore_helper.set_property_filter(ds.Filter(), 'Location.Lat', PropertyFilter.GREATER_THAN_OR_EQUAL, 5.4995),
        # datastore_helper.set_property_filter(ds.Filter(), 'Location.Lat', PropertyFilter.LESS_THAN_OR_EQUAL, 83.1621),
        # datastore_helper.set_property_filter(ds.Filter(), 'Location.Lng', PropertyFilter.GREATER_THAN_OR_EQUAL, -167.2764),
    datastore_helper.set_property_filter(ds.Filter(), 'Location.Lng', PropertyFilter.LESS_THAN_OR_EQUAL, -52.2330)
    # )

    lines = p \
        | 'ReadDatastoreOccurrences' >> ReadFromDatastore(project=project, query=q, num_splits=0) \
        | 'ConvertEntitiesToOccurrences' >> beam.ParDo(EntityToOccurrence()) \
        | 'RemoveDuplicates' >> beam.RemoveDuplicates() \
        | 'ParseKeyValue' >> beam.ParDo(GroupTaxa()) \
        | 'GroupByTaxon' >> beam.GroupByKey() \
        | 'UnwindSufficientTaxa' >> beam.ParDo(UnwindTaxaWithSufficientCount()) \
        | 'FormatToWrite' >> beam.Map(lambda o: o.example.SerializeToString())

    lines | 'write' >> WriteToText(known_args.output)

    result = p.run()

    result.wait_until_finish()

    for v in ['new_occurrences', 'invalid_occurrences', 'sufficient_taxa', 'insufficient_taxa', 'final_occurrences']:
        query_result = result.metrics().query(MetricsFilter().with_name(v))
        if query_result['counters']:
            logging.info('%s: %d', v, query_result['counters'][0].committed)

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()