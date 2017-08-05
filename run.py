from __future__ import absolute_import

import argparse
import apache_beam as beam
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.io.gcp.datastore.v1.datastoreio import ReadFromDatastore
import logging
from google.cloud.proto.datastore.v1 import query_pb2
from googledatastore import helper as datastore_helper, PropertyFilter
import googledatastore as ds
from apache_beam.metrics import Metrics
from apache_beam.metrics.metric import MetricsFilter
import occurrence as fo

@beam.typehints.with_input_types(fo.Occurrence)
@beam.typehints.with_output_types(beam.typehints.KV[int, fo.Occurrence])
class GroupTaxa(beam.DoFn):
    def __init__(self):
        super(GroupTaxa, self).__init__()

    def process(self, o):
        yield o.example.features.feature['label'].int64_list.value[0], o


@beam.typehints.with_input_types(beam.typehints.KV[int, beam.typehints.Iterable[fo.Occurrence]])
@beam.typehints.with_output_types(fo.Occurrence)
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
    # https://stackoverflow.com/questions/41705870/geospatial-query-at-google-app-engine-datastore
    # The query index may not be implemented at this point.
    datastore_helper.set_property_filter(ds.Filter(), 'Location.longitude', PropertyFilter.LESS_THAN_OR_EQUAL, -52.2330)
    # )

    lines = p \
        | 'ReadDatastoreOccurrences' >> ReadFromDatastore(project=project, query=q, num_splits=0) \
        | 'ConvertEntitiesToOccurrences' >> beam.ParDo(fo.EntityToOccurrence()) \
        | 'RemoveDuplicates' >> beam.RemoveDuplicates() \
        | 'ParseKeyValue' >> beam.ParDo(GroupTaxa()) \
        | 'GroupByTaxon' >> beam.GroupByKey() \
        | 'UnwindSufficientTaxa' >> beam.ParDo(UnwindTaxaWithSufficientCount()) \
        | 'FormatToWrite' >> beam.Map(lambda o: o.example.SerializeToString())

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
        'final_occurrences'
    ]:
        query_result = result.metrics().query(MetricsFilter().with_name(v))
        if query_result['counters']:
            logging.info('%s: %d', v, query_result['counters'][0].committed)

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()