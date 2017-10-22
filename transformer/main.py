# from __future__ import absolute_import
from __future__ import division
import logging

def main(argv=None):
    from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions, SetupOptions
    # If error after upgradeing apache beam: metaclass conflict: the metaclass of a derived class must be a (non-strict) subclass of the metaclasses of all its bases
    # then: pip install six==1.10.0
    from tensorflow_transform.beam import tft_beam_io
    from tensorflow_transform.beam import impl as tft
    from tensorflow_transform import coders
    from tensorflow_transform.tf_metadata import dataset_metadata
    import os
    from datetime import datetime
    from apache_beam.io.filesystem import CompressionTypes
    import apache_beam as beam
    from transformer import options, functions

    pipeline_options = PipelineOptions(flags=argv)
    # ['--setup_file', os.path.abspath(os.path.join(os.path.dirname(__file__), 'setup.py'))],
    # )

    transformer_pipeline_options = pipeline_options.view_as(options.ProcessPipelineOptions)
    cloud_options = pipeline_options.view_as(GoogleCloudOptions)
    cloud_options.project = options._default_project()
    standard_options = pipeline_options.view_as(StandardOptions)
    pipeline_options.view_as(SetupOptions).setup_file = os.path.abspath(os.path.join(os.path.dirname(__file__), 'setup.py'))

    train_directory_path = os.path.join(transformer_pipeline_options.train_location, datetime.now().strftime("%s"))

    with beam.Pipeline(standard_options.runner, options=pipeline_options) as pipeline:
        with tft.Context(temp_dir=cloud_options.temp_location):

            RAW_METADATA_DIR = 'raw_metadata'
            TRANSFORMED_TRAIN_DATA_FILE_PREFIX = 'train'
            TRANSFORMED_EVAL_DATA_FILE_PREFIX = 'eval'

            input_schema = functions.make_input_schema(transformer_pipeline_options.mode)
            input_coder = coders.ExampleProtoCoder(input_schema)

            records = pipeline \
                      | 'ReadInitialTFRecords' >> beam.io.ReadFromTFRecord(
                transformer_pipeline_options.raw_data_location+"/*.gz",
                compression_type=CompressionTypes.GZIP) \
                      | 'DecodeProtoExamples' >> beam.Map(input_coder.decode)

            # records = records | 'RecordsDataset' >> beam.ParDo(occurrences.Counter("main"))

            preprocessing_fn = functions.make_preprocessing_fn(transformer_pipeline_options.num_classes)
            metadata = dataset_metadata.DatasetMetadata(schema=input_schema)

            _ = metadata \
                | 'WriteInputMetadata' >> tft_beam_io.WriteMetadata(
                path=os.path.join(train_directory_path, RAW_METADATA_DIR),
                pipeline=pipeline)

            (records_dataset, records_metadata), transform_fn = (
                (records, metadata) | tft.AnalyzeAndTransformDataset(preprocessing_fn))

            _ = records_dataset \
                | 'ProjectLabels' >> beam.Map(lambda e: e["taxon"]) \
                | 'RemoveLabelDuplicates' >> beam.RemoveDuplicates() \
                | 'WriteLabels' >> beam.io.WriteToText(train_directory_path+"/labels", file_name_suffix='.txt')

            _ = (transform_fn
                 | 'WriteTransformFn' >> tft_beam_io.WriteTransformFn(train_directory_path))

            random_seed = int(datetime.now().strftime("%s"))
            def train_eval_partition_fn(ex, unused_num_partitions):
                return functions.partition_fn(ex, random_seed, transformer_pipeline_options.percent_eval)

            train_dataset, eval_dataset = records_dataset \
                                          | 'TrainEvalPartition' >> beam.Partition(train_eval_partition_fn, 2)

            coder = coders.ExampleProtoCoder(records_metadata.schema)
            _ = train_dataset \
                | 'SerializeTrainExamples' >> beam.Map(coder.encode) \
                | 'ShuffleTraining' >> functions.Shuffle() \
                | 'WriteTraining' >> beam.io.WriteToTFRecord(
                os.path.join(train_directory_path+"/train_data/", TRANSFORMED_TRAIN_DATA_FILE_PREFIX),
                file_name_suffix='.tfrecord.gz')

            _ = eval_dataset \
                | 'SerializeEvalExamples' >> beam.Map(coder.encode) \
                | 'ShuffleEval' >> functions.Shuffle() \
                | 'WriteEval' >> beam.io.WriteToTFRecord(
                os.path.join(train_directory_path+"/eval_data/", TRANSFORMED_EVAL_DATA_FILE_PREFIX),
                file_name_suffix='.tfrecord.gz')

            _ = train_dataset \
                | 'CountTraining' >> beam.combiners.Count.Globally() \
                | 'WriteTrainCount' >> beam.io.WriteToText(
                os.path.join(train_directory_path, 'train_count'), file_name_suffix=".txt")

            _ = eval_dataset \
                | 'CountEval' >> beam.combiners.Count.Globally() \
                | 'WriteEvalCount' >> beam.io.WriteToText(
                os.path.join(train_directory_path, 'eval_count'), file_name_suffix=".txt")


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    main()