# from __future__ import absolute_import

import apache_beam as beam

def preprocess_train(
        pipeline_options,
        intermediate_records,
        output_path,
):
    import example as example
    from tensorflow_transform.beam import tft_beam_io
    from tensorflow_transform.beam import impl as tft
    from tensorflow_transform import coders
    from tensorflow_transform.tf_metadata import dataset_metadata
    import os
    import utils as utils
    from datetime import datetime
    from apache_beam.io.filesystem import CompressionTypes

    options = pipeline_options.get_all_options()

    with beam.Pipeline(options['runner'], options=pipeline_options) as pipeline:
        with tft.Context(temp_dir=options['temp_location']):

            RAW_METADATA_DIR = 'raw_metadata'
            TRANSFORMED_TRAIN_DATA_FILE_PREFIX = 'train'
            TRANSFORMED_EVAL_DATA_FILE_PREFIX = 'eval'

            input_schema = example.make_input_schema(options['mode'])
            input_coder = coders.ExampleProtoCoder(input_schema)

            records = pipeline \
                | 'ReadInitialTFRecords' >> beam.io.ReadFromTFRecord(
                        intermediate_records+"/*.gz",
                        compression_type=CompressionTypes.GZIP) \
                | 'DecodeProtoExamples' >> beam.Map(input_coder.decode)

            # records = records | 'RecordsDataset' >> beam.ParDo(occurrences.Counter("main"))

            preprocessing_fn = example.make_preprocessing_fn(options['num_classes'])
            metadata = dataset_metadata.DatasetMetadata(schema=input_schema)

            _ = metadata \
                | 'WriteInputMetadata' >> tft_beam_io.WriteMetadata(
                    path=os.path.join(output_path, RAW_METADATA_DIR),
                    pipeline=pipeline)

            (records_dataset, records_metadata), transform_fn = (
                (records, metadata) | tft.AnalyzeAndTransformDataset(preprocessing_fn))

            _ = (transform_fn
                 | 'WriteTransformFn' >> tft_beam_io.WriteTransformFn(output_path))

            random_seed = int(datetime.now().strftime("%s"))
            def train_eval_partition_fn(ex, unused_num_partitions):
                return partition_fn(ex, random_seed, options['percent_eval'])

            train_dataset, eval_dataset = records_dataset \
                | 'TrainEvalPartition' >> beam.Partition(train_eval_partition_fn, 2)

            coder = coders.ExampleProtoCoder(records_metadata.schema)
            _ = train_dataset \
                | 'SerializeTrainExamples' >> beam.Map(coder.encode) \
                | 'ShuffleTraining' >> utils.Shuffle() \
                | 'WriteTraining' >> beam.io.WriteToTFRecord(
                        os.path.join(output_path, "/train_data/", TRANSFORMED_TRAIN_DATA_FILE_PREFIX),
                        file_name_suffix='.tfrecord.gz')

            _ = eval_dataset \
                | 'SerializeEvalExamples' >> beam.Map(coder.encode) \
                | 'ShuffleEval' >> utils.Shuffle() \
                | 'WriteEval' >> beam.io.WriteToTFRecord(
                        os.path.join(output_path, "/eval_data/", TRANSFORMED_EVAL_DATA_FILE_PREFIX),
                        file_name_suffix='.tfrecord.gz')

            _ = train_dataset \
                          | 'CountTraining' >> beam.combiners.Count.Globally() \
                          | 'WriteTrainCount' >> beam.io.WriteToText(
                                os.path.join(output_path, 'train_count'), file_name_suffix=".txt")

            _ = eval_dataset \
                | 'CountEval' >> beam.combiners.Count.Globally() \
                | 'WriteEvalCount' >> beam.io.WriteToText(
                        os.path.join(output_path, 'eval_count'), file_name_suffix=".txt")


# def partition_fn(user_id, partition_random_seed, percent_eval):
# I hope taxon_id will provide wide enough variation between results.
def partition_fn(ex, partition_random_seed, percent_eval):
    import hashlib
    m = hashlib.md5(str(ex["occurrence_id"][0]) + str(partition_random_seed))
    hash_value = int(m.hexdigest(), 16) % 100
    return 0 if hash_value >= percent_eval else 1