from __future__ import absolute_import

import argparse
import apache_beam as beam
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.io.gcp.datastore.v1.datastoreio import ReadFromDatastore
import logging
from google.cloud.datastore import query
from google.cloud.proto.datastore.v1 import query_pb2
from googledatastore import helper as datastore_helper, PropertyFilter
import googledatastore as ds
from apache_beam.metrics import Metrics
from apache_beam.metrics.metric import MetricsFilter
import occurrence as fo
import weather as fw
import tensorflow as tf

@beam.typehints.with_input_types(beam.typehints.KV[int, beam.typehints.Iterable[tf.train.SequenceExample]])
@beam.typehints.with_output_types(tf.train.SequenceExample)
class UnwindTaxaWithSufficientCount(beam.DoFn):
    def __init__(self):
        super(UnwindTaxaWithSufficientCount, self).__init__()
        self.sufficient_taxa_counter = Metrics.counter('main', 'sufficient_taxa')
        self.insufficient_taxa_counter = Metrics.counter('main', 'insufficient_taxa')

    def process(self, element):
        if len(element[1]) <= 50:
            self.insufficient_taxa_counter.inc()
            return

        self.sufficient_taxa_counter.inc()

        logging.info("%d: %d", element[0], len(element[1]))

        for o in element[1]:
            yield o


@beam.typehints.with_input_types(tf.train.SequenceExample)
@beam.typehints.with_output_types(str)
class FormatToWrite(beam.DoFn):
    def __init__(self):
        super(FormatToWrite, self).__init__()
        self.final_occurrence_count = Metrics.counter('main', 'final_occurrence_count')

    def process(self, element):
        self.final_occurrence_count.inc()
        yield element.SerializeToString()

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


    # q = query.Query(kind='Occurrence', project=project)
    # q.fetch()
    q = query_pb2.Query()
    datastore_helper.set_kind(q, 'Occurrence')
    # q.limit.name = 100

    # Need to index this in google.

    # datastore_helper.set_composite_filter(q.filter, CompositeFilter.AND,
        # datastore_helper.set_property_filter(ds.Filter(), 'Location.Lat', PropertyFilter.GREATER_THAN_OR_EQUAL, 5.4995),
        # datastore_helper.set_property_filter(ds.Filter(), 'Location.Lat', PropertyFilter.LESS_THAN_OR_EQUAL, 83.1621),
        # datastore_helper.set_property_filter(ds.Filter(), 'Location.Lng', PropertyFilter.GREATER_THAN_OR_EQUAL, -167.2764),
    # https://stackoverflow.com/questions/41705870/geospatial-query-at-google-app-engine-datastore
    # The query index may not be implemented at this point.
    datastore_helper.set_property_filter(ds.Filter(), 'Location.longitude', PropertyFilter.LESS_THAN_OR_EQUAL, -52.2330)
    # )

    lines = p \
        | 'ReadDatastoreOccurrences' >> ReadFromDatastore(project=project, query=q, num_splits=0) \
        | 'ConvertEntitiesToKeyStrings' >> beam.ParDo(fo.EntityToString()) \
        | 'RemoveDuplicates' >> beam.RemoveDuplicates() \
        | 'ConvertKeyStringsToTaxonSequenceExample' >> beam.ParDo(fo.StringToTaxonSequenceExample()) \
        | 'GroupByTaxon' >> beam.GroupByKey() \
        | 'UnwindSufficientTaxa' >> beam.ParDo(UnwindTaxaWithSufficientCount()) \
        | 'AttachWeather' >> beam.ParDo(fw.FetchWeatherDoFn(project)) \
        | 'FormatToWrite' >> beam.ParDo(FormatToWrite())

    lines | 'write' >> WriteToText(known_args.output)

    result = p.run()

    result.wait_until_finish()

    for v in [
        'new_occurrences',
        'invalid_occurrence_location',
        'invalid_occurrence_date',
        'sufficient_taxa',
        'insufficient_taxa',
        'insufficient_weather_records',
        'final_occurrence_count'
    ]:
        query_result = result.metrics().query(MetricsFilter().with_name(v))
        if query_result['counters']:
            logging.info('%s: %d', v, query_result['counters'][0].committed)

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()