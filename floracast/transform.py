# from __future__ import absolute_import
from __future__ import division

import logging
from tensorflow.contrib.learn import ModeKeys
import apache_beam as beam
from apache_beam.io.filesystem import CompressionTypes
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions, SetupOptions
from tensorflow_transform.beam import impl as tft
# If error after upgradeing apache beam: metaclass conflict: the metaclass of a derived class must be a (non-strict) subclass of the metaclasses of all its bases
# then: pip install six==1.10.0
from tensorflow_transform.beam import tft_beam_io
from functions.transform import TransformData, FetchExampleFiles
from apache_beam.io.tfrecordio import _TFRecordSource, _TFRecordUtil

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

        # Intermediate TFRecords are stored in their own directory, each with a corresponding metadata file.
        # The metadata lists how many records, how many of each taxon label.

        # parser.add_argument(
        #     '--output_location',
        #     required=True,
        #     type=str,
        #     help='The location to write transformed tfrecords'
        # )
        #
        # parser.add_argument(
        #     '--bucket',
        #     required=False,
        #     type=str,
        #     help='The GCS Bucket.'
        # )

        parser.add_argument(
            '--bucket',
            required=False,
            type=str,
            help='train, eval, infer'
        )

        # parser.add_argument(
        #     '--percent_eval',
        #     required=True,
        #     default=10,
        #     type=int,
        #     help='Percentage to use for testing.'
        # )

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

# def parse_taxon_timestamp_from_occurrence_path(p):
#     if p.endswith("/"):
#         p = p[:-1]
#
#     s = p.split("/")
#
#     return s[len(s) - 2], s[len(s) - 1]


# def filterRandomToMatchOccurrenceCount(random_example, occurrence_count):
#     b_info = random_example.example_id().split("-")
#     b_size = b_info[2]
#     b_number = b_info[0]
#     if b_number <= ceil(occurrence_count / b_size):
#         yield random_example


class ReadExamples(beam.DoFn):
    def __init__(self):
        super(ReadExamples, self).__init__()

    def process(self, file_name, coder):

        src = _TFRecordSource(file_pattern=file_name,
                        compression_type=CompressionTypes.UNCOMPRESSED,
                        coder=coder,
                        validate=True)

        with src.open_file(file_name) as file_handle:
            while True:
                record = _TFRecordUtil.read_record(file_handle)
                if record is None:
                    return  # Reached EOF
                else:
                    yield coder.decode(record)


def main(argv=None):

    transform_data = TransformData()

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

            examples = pipeline \
                     | 'SetFileFetcherInMotion' >> beam.Create([1]).with_output_types(int) \
                     | 'FetchLatestFiles' >> beam.ParDo(FetchExampleFiles(
                                project=cloud_options.project,
                                bucket=local_options.bucket,
                            )) \
                     | 'ReadExamples' >> beam.ParDo(ReadExamples(), transform_data.coder)

            # occurrence_count = occurrences | beam.combiners.Count.Globally() >> pvalue.AsSingleton(occurrence_count)

            _ = transform_data.create_raw_metadata(ModeKeys.TRAIN) \
                | 'WriteInputMetadata' >> tft_beam_io.WriteMetadata(
                path=transform_data.raw_metadata_path,
                pipeline=pipeline)

            (transformed_dataset, transformed_metadata), transform_fn = (
                (examples, transform_data.create_raw_metadata(ModeKeys.TRAIN)) | tft.AnalyzeAndTransformDataset(transform_data.make_preprocessing_fn()))

            _ = (transform_fn
                 | 'WriteTransformFn' >> tft_beam_io.WriteTransformFn(transform_data.output_path))

            # random_seed = int(datetime.now().strftime("%s"))
            # def train_eval_partition_fn(ex, unused_num_partitions):
            #     return utils.partition_fn(ex, random_seed, local_options.percent_eval)
            #
            # train_dataset, eval_dataset = records \
            #                               | 'TrainEvalPartition' >> beam.Partition(train_eval_partition_fn, 2)

            # coder = coders.ExampleProtoCoder(transformed_metadata.schema)

            # _ = examples \
            #     | 'SerializeTrainExamples' >> beam.Map(example_coder.encode) \
            #     | 'ShuffleTraining' >> utils.Shuffle() \
            #     | 'WriteTraining' >> beam.io.WriteToTFRecord(
            #
            #     os.path.join(local_options.output_location, "examples", TRANSFORMED_TRAIN_DATA_FILE_PREFIX),
            #     file_name_suffix='.tfrecord.gz')



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