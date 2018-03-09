# from __future__ import absolute_import
from __future__ import division

import logging
import os
from tensorflow.contrib.learn import ModeKeys
import apache_beam as beam
from apache_beam import pvalue
from apache_beam.io.filesystem import CompressionTypes
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions, SetupOptions
from tensorflow_transform import coders
from tensorflow_transform.beam import impl as tft
# If error after upgradeing apache beam: metaclass conflict: the metaclass of a derived class must be a (non-strict) subclass of the metaclasses of all its bases
# then: pip install six==1.10.0
from tensorflow_transform.beam import tft_beam_io
from functions import transform, utils
from math import ceil

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
            '--occurrence_file',
            required=True,
            type=str,
            help='The GCS TFRecord file of Occurrences.'
        )

        parser.add_argument(
            '--random_file',
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
        except ValueError as err:
            #TODO: LOG THIS ERROR!
            print(err)
            return
            # self._counter = self._counter + 1
            # print("invalid", self._counter)

def parse_taxon_timestamp_from_occurrence_path(p):
    if p.endswith("/"):
        p = p[:-1]

    s = p.split("/")

    return s[len(s) - 2], s[len(s) - 1]


def filterRandomToMatchOccurrenceCount(random_example, occurrence_count):
    b_info = random_example.example_id().split("-")
    b_size = b_info[2]
    b_number = b_info[0]
    if b_number <= ceil(occurrence_count / b_size):
        yield random_example



def main(argv=None):

    RAW_METADATA_DIR = 'raw_metadata'
    TRANSFORMED_TRAIN_DATA_FILE_PREFIX = 'train'
    TRANSFORMED_EVAL_DATA_FILE_PREFIX = 'eval'
    raw_metadata = transform.create_raw_metadata(ModeKeys.TRAIN)
    example_converter = coders.ExampleProtoCoder(raw_metadata.schema)

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

            occurrences = pipeline \
                      | 'ReadOccurrenceTFRecords' >> beam.io.ReadFromTFRecord(
                file_pattern=local_options.occurrence_file,
                compression_type=CompressionTypes.UNCOMPRESSED,
            ) \
                      | 'DecodeOccurrenceProtoExamples' >> beam.ParDo(Decoder(), example_converter.decode)

            occurrence_count = occurrences | beam.combiners.Count.Globally()

            random_records = pipeline \
                             | 'ReadRandomTFRecords' >> beam.io.ReadFromTFRecord(
                                file_pattern=local_options.random_file,
                                compression_type=CompressionTypes.UNCOMPRESSED,
            ) \
                             | 'DecodeRandomProtoExamples' >> beam.ParDo(Decoder(), example_converter.decode) \
                             | 'FilterRandomCloserToOccurrenceCount' >> beam.Filter(filterRandomToMatchOccurrenceCount, pvalue.AsSingleton(occurrence_count))

            records = (occurrences, random_records) | beam.Flatten()

            # records = records | 'RecordsDataset' >> beam.ParDo(occurrences.Counter("main"))

            # preprocessing_fn = functions.make_preprocessing_fn(transformer_pipeline_options.num_classes)
            preprocessing_fn = transform.make_preprocessing_fn()

            _ = raw_metadata \
                | 'WriteInputMetadata' >> tft_beam_io.WriteMetadata(
                path=os.path.join(local_options.output_location, RAW_METADATA_DIR),
                pipeline=pipeline)

            (transformed_dataset, transformed_metadata), transform_fn = (
                (records, raw_metadata) | tft.AnalyzeAndTransformDataset(preprocessing_fn))

            # _ = transformed_dataset \
            #     | 'ProjectLabels' >> beam.Map(lambda e: e["taxon"]) \
            #     | 'RemoveLabelDuplicates' >> beam.RemoveDuplicates() \
            #     | 'WriteLabels' >> beam.io.WriteToText(local_options.output_location, file_name_suffix='labels.txt')

            _ = (transform_fn
                 | 'WriteTransformFn' >> tft_beam_io.WriteTransformFn(local_options.output_location))

            # random_seed = int(datetime.now().strftime("%s"))
            # def train_eval_partition_fn(ex, unused_num_partitions):
            #     return utils.partition_fn(ex, random_seed, local_options.percent_eval)
            #
            # train_dataset, eval_dataset = records \
            #                               | 'TrainEvalPartition' >> beam.Partition(train_eval_partition_fn, 2)

            # coder = coders.ExampleProtoCoder(transformed_metadata.schema)

            _ = records \
                | 'SerializeTrainExamples' >> beam.Map(example_converter.encode) \
                | 'ShuffleTraining' >> utils.Shuffle() \
                | 'WriteTraining' >> beam.io.WriteToTFRecord(
                os.path.join(local_options.output_location, "examples", TRANSFORMED_TRAIN_DATA_FILE_PREFIX),
                file_name_suffix='.tfrecord.gz')



            # _ = train_dataset \
            #     | 'SerializeTrainExamples' >> beam.Map(example_converter.encode) \
            #     | 'ShuffleTraining' >> utils.Shuffle() \
            #     | 'WriteTraining' >> beam.io.WriteToTFRecord(
            #         os.path.join(local_options.output_location, "train_data", TRANSFORMED_TRAIN_DATA_FILE_PREFIX),
            #         file_name_suffix='.tfrecord.gz')
            #
            # _ = eval_dataset \
            #     | 'SerializeEvalExamples' >> beam.Map(example_converter.encode) \
            #     | 'ShuffleEval' >> utils.Shuffle() \
            #     | 'WriteEval' >> beam.io.WriteToTFRecord(
            #         os.path.join(local_options.output_location, "eval_data", TRANSFORMED_EVAL_DATA_FILE_PREFIX),
            #         file_name_suffix='.tfrecord.gz')
            #
            # _ = train_dataset \
            #     | 'CountTraining' >> beam.combiners.Count.Globally() \
            #     | 'WriteTrainCount' >> beam.io.WriteToText(
            #         local_options.output_location + "/train_count",
            #         file_name_suffix=".txt")
            #
            # _ = eval_dataset \
            #     | 'CountEval' >> beam.combiners.Count.Globally() \
            #     | 'WriteEvalCount' >> beam.io.WriteToText(
            #         local_options.output_location + "/eval_count",
            #         file_name_suffix=".txt")


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    main()