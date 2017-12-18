# from __future__ import absolute_import
from __future__ import division

import logging
import os
from datetime import datetime

import apache_beam as beam
from apache_beam import pvalue
from apache_beam.io.filesystem import CompressionTypes
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions, SetupOptions
from tensorflow_transform import coders
from tensorflow_transform.beam import impl as tft
# If error after upgradeing apache beam: metaclass conflict: the metaclass of a derived class must be a (non-strict) subclass of the metaclasses of all its bases
# then: pip install six==1.10.0
from tensorflow_transform.beam import tft_beam_io
from transform import functions

def _default_project():
    import os
    import subprocess
    get_project = [
        'gcloud', 'config', 'list', 'project', '--format=value(core.project)'
    ]

    with open(os.devnull, 'w') as dev_null:
        return subprocess.check_output(get_project, stderr=dev_null).strip()

class LocalPipelineOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):

        #### FETCH ####

        # Intermediate TFRecords are stored in their own directory, each with a corresponding metadata file.
        # The metadata lists how many records, how many of each taxon label.
        parser.add_argument(
            '--output_location',
            required=True,
            type=str,
            help='The location to write transformed tfrecords'
        )

        parser.add_argument(
            '--occurrence_location',
            required=True,
            type=str,
            help='The location of occurrence tfrecords.'
        )

        parser.add_argument(
            '--random_location',
            required=True,
            type=str,
            help='The location of random tfrecords.'
        )

        parser.add_argument(
            '--mode',
            required=True,
            type=str,
            help='train, eval, infer'
        )

        parser.add_argument(
            '--percent_eval',
            required=True,
            default=10,
            type=int,
            help='Percentage to use for testing.'
        )

class Decoder(beam.DoFn):

    def process(self, element, decoder):
        try:
            decoded = decoder(element)
            # print(decoded)
            yield decoded
        except ValueError:
            #TODO: LOG THIS ERROR!
            return
            # self._counter = self._counter + 1
            # print("invalid", self._counter)

def parse_taxon_timestamp_from_occurrence_path(p):
    if p.endswith("/"):
        p = p[:-1]

    s = p.split("/")

    return s[len(s) - 2], s[len(s) - 1]

def main(argv=None):

    pipeline_options = PipelineOptions(flags=argv)
    # ['--setup_file', os.path.abspath(os.path.join(os.path.dirname(__file__), 'setup.py'))],
    # )

    local_options = pipeline_options.view_as(LocalPipelineOptions)
    cloud_options = pipeline_options.view_as(GoogleCloudOptions)
    cloud_options.project = _default_project()
    standard_options = pipeline_options.view_as(StandardOptions)
    pipeline_options.view_as(SetupOptions).save_main_session = True

    with beam.Pipeline(standard_options.runner, options=pipeline_options) as pipeline:
        with tft.Context(temp_dir=cloud_options.temp_location):

            print("OUTPUT", local_options.output_location)

            RAW_METADATA_DIR = 'raw_metadata'
            TRANSFORMED_TRAIN_DATA_FILE_PREFIX = 'train'
            TRANSFORMED_EVAL_DATA_FILE_PREFIX = 'eval'

            RAW_DATA_METADATA = functions.create_raw_metadata(local_options.mode)
            converter = coders.ExampleProtoCoder(RAW_DATA_METADATA.schema)

            occurrences = pipeline \
                      | 'ReadOccurrenceTFRecords' >> beam.io.ReadFromTFRecord(
                local_options.occurrence_location + "/*.gz",
                compression_type=CompressionTypes.GZIP) \
                      | 'DecodeOccurrenceProtoExamples' >> beam.ParDo(Decoder(), converter.decode)

            occurrence_count = occurrences | beam.combiners.Count.Globally()

            random_records = pipeline \
                             | 'ReadRandomTFRecords' >> beam.io.ReadFromTFRecord(
                                local_options.random_location+"/*.gz",
                                compression_type=CompressionTypes.GZIP) \
                             | 'DecodeRandomProtoExamples' >> beam.ParDo(Decoder(), converter.decode) \
                             | 'ShuffleRandom' >> functions.Shuffle() \
                             | 'PairWithStandardToGroupToTruncate' >> beam.Map(lambda e: ('_', e)) \
                             | 'GroupByKey' >> beam.GroupByKey() \
                             | 'TruncateRandomToOccurrenceCount' >> beam.ParDo(functions.Truncate(), pvalue.AsSingleton(occurrence_count))

            records = (occurrences, random_records) | beam.Flatten()

            # records = records | 'RecordsDataset' >> beam.ParDo(occurrences.Counter("main"))

            # preprocessing_fn = functions.make_preprocessing_fn(transformer_pipeline_options.num_classes)
            preprocessing_fn = functions.make_preprocessing_fn(num_classes=2)

            _ = RAW_DATA_METADATA \
                | 'WriteInputMetadata' >> tft_beam_io.WriteMetadata(
                path=os.path.join(local_options.output_location, RAW_METADATA_DIR),
                pipeline=pipeline)

            (transformed_dataset, transformed_metadata), transform_fn = (
                (records, RAW_DATA_METADATA) | tft.AnalyzeAndTransformDataset(preprocessing_fn))

            # _ = transformed_dataset \
            #     | 'ProjectLabels' >> beam.Map(lambda e: e["taxon"]) \
            #     | 'RemoveLabelDuplicates' >> beam.RemoveDuplicates() \
            #     | 'WriteLabels' >> beam.io.WriteToText(local_options.output_location, file_name_suffix='labels.txt')

            _ = (transform_fn
                 | 'WriteTransformFn' >> tft_beam_io.WriteTransformFn(local_options.output_location))

            random_seed = int(datetime.now().strftime("%s"))
            def train_eval_partition_fn(ex, unused_num_partitions):
                return functions.partition_fn(ex, random_seed, local_options.percent_eval)

            train_dataset, eval_dataset = records \
                                          | 'TrainEvalPartition' >> beam.Partition(train_eval_partition_fn, 2)

            # coder = coders.ExampleProtoCoder(transformed_metadata.schema)
            _ = train_dataset \
                | 'SerializeTrainExamples' >> beam.Map(converter.encode) \
                | 'ShuffleTraining' >> functions.Shuffle() \
                | 'WriteTraining' >> beam.io.WriteToTFRecord(
                    os.path.join(local_options.output_location, "train_data", TRANSFORMED_TRAIN_DATA_FILE_PREFIX),
                    file_name_suffix='.tfrecord.gz')

            _ = eval_dataset \
                | 'SerializeEvalExamples' >> beam.Map(converter.encode) \
                | 'ShuffleEval' >> functions.Shuffle() \
                | 'WriteEval' >> beam.io.WriteToTFRecord(
                    os.path.join(local_options.output_location, "eval_data", TRANSFORMED_EVAL_DATA_FILE_PREFIX),
                    file_name_suffix='.tfrecord.gz')

            _ = train_dataset \
                | 'CountTraining' >> beam.combiners.Count.Globally() \
                | 'WriteTrainCount' >> beam.io.WriteToText(
                    local_options.output_location + "/train_count",
                    file_name_suffix=".txt")

            _ = eval_dataset \
                | 'CountEval' >> beam.combiners.Count.Globally() \
                | 'WriteEvalCount' >> beam.io.WriteToText(
                    local_options.output_location + "/eval_count",
                    file_name_suffix=".txt")


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    main()