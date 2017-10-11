# from __future__ import absolute_import
from __future__ import division
import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

KEY_OCCURRENCE_ID = 'occurrence_id'
KEY_TAXON = 'taxon'
KEY_LATITUDE = 'latitude'
KEY_LONGITUDE = 'longitude'
KEY_ELEVATION = 'elevation'
KEY_DATE ='date'
KEY_AVG_TEMP = 'avg_temp'
KEY_MAX_TEMP = 'max_temp'
KEY_MIN_TEMP = 'min_temp'
KEY_PRCP = 'precipitation'
KEY_DAYLIGHT = 'daylight'
KEY_GRID_ZONE = 'mgrs_grid_zone'

class ProcessPipelineOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):


        #### GENERAL ####


        parser.add_argument(
            '--mode',
            required=True,
            help='eval, train, or infer which defines the pipeline to run.')


        #### FETCH ####

        # The intermediate filenames are unix timestamps.
        # If not specified new occurrence data will be fetched and a new timestamp given.
        parser.add_argument(
            '--raw_data_location',
            required=False,
            default=None,
            help='A unix timestamp representing the fetched time of occurrences, the directory in which tfrecords are stored.'
        )

        parser.add_argument(
            '--minimum_occurrences_within_taxon',
            required=False,
            default=40,
            help='The number of occurrence required to process taxon')

        parser.add_argument(
            '--occurrence_taxa',
            required=False,
            default=None,
            type=list,
            help='Restrict occurrence fetch to this taxa')

        parser.add_argument(
            '--weather_station_distance',
            required=False,
            default=75,
            help='Maximum distance a weather station can be from an occurrence when fetching weather.')


        #### TRAIN ####

        parser.add_argument(
            '--percent_eval',
            required=False,
            default=10,
            type=int,
            help='Directory that contains timestamped files for each training iteration')

        parser.add_argument(
            '--train_location',
            required=False,
            help='Directory that contains timestamped files for each training iteration')


        # Google cloud options.
        # parser.add_argument(
        #     '--temp_location',
        #     required=True,
        #     help='Temporary data')
        #
        # parser.add_argument(
        #     '--staging_location',
        #     required=True,
        #     help='Staging data')

        parser.add_argument(
            '--num_classes',
            required=False,
            type=int,
            help='Number of training classes')


        #### INFER ####


        parser.add_argument(
            '--infer_location',
            required=False,
            help='Directory that contains timestamped files for collected infer data. Should be similar meta to fetched format.')

        parser.add_argument(
            '--weeks_before',
            required=False,
            default=1,
            type=int,
            help='The number of weeks in the past to generate prediction data for each forest'
            # If the model changes, we can expect this to be 52 weeks in the past. If not, just this week,
            # calculated every Friday.
        )

        parser.add_argument(
            '--protected_area_count',
            required=False,
            default=0,
            type=int,
            help='The number of locations to generate data for'
            # If the model changes, we can expect this to be 52 weeks in the past. If not, just this week,
            # calculated every Friday.
        )

        parser.add_argument(
            '--add_random_train_point',
            required=False,
            default=True,
            help='Should a random training location be added for every actual occurrence?')
        # pip install "apache_beam[gcp]"

def _default_project():
    import os
    import subprocess
    get_project = [
        'gcloud', 'config', 'list', 'project', '--format=value(core.project)'
    ]

    with open(os.devnull, 'w') as dev_null:
        return subprocess.check_output(get_project, stderr=dev_null).strip()


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


    pipeline_options = PipelineOptions(flags=argv)
    # ['--setup_file', os.path.abspath(os.path.join(os.path.dirname(__file__), 'setup.py'))],
    # )

    transformer_pipeline_options = pipeline_options.view_as(ProcessPipelineOptions)
    cloud_options = pipeline_options.view_as(GoogleCloudOptions)
    cloud_options.project = _default_project()
    standard_options = pipeline_options.view_as(StandardOptions)
    pipeline_options.view_as(SetupOptions).setup_file = os.path.abspath(os.path.join(os.path.dirname(__file__), 'setup.py'))

    train_directory_path = os.path.join(transformer_pipeline_options.train_location, datetime.now().strftime("%s"))

    with beam.Pipeline(standard_options.runner, options=pipeline_options) as pipeline:
        with tft.Context(temp_dir=cloud_options.temp_location):

            RAW_METADATA_DIR = 'raw_metadata'
            TRANSFORMED_TRAIN_DATA_FILE_PREFIX = 'train'
            TRANSFORMED_EVAL_DATA_FILE_PREFIX = 'eval'

            input_schema = make_input_schema(transformer_pipeline_options.mode)
            input_coder = coders.ExampleProtoCoder(input_schema)

            records = pipeline \
                      | 'ReadInitialTFRecords' >> beam.io.ReadFromTFRecord(
                transformer_pipeline_options.raw_data_location+"/*.gz",
                compression_type=CompressionTypes.GZIP) \
                      | 'DecodeProtoExamples' >> beam.Map(input_coder.decode)

            # records = records | 'RecordsDataset' >> beam.ParDo(occurrences.Counter("main"))

            preprocessing_fn = make_preprocessing_fn(transformer_pipeline_options.num_classes)
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
                return partition_fn(ex, random_seed, transformer_pipeline_options.percent_eval)

            train_dataset, eval_dataset = records_dataset \
                                          | 'TrainEvalPartition' >> beam.Partition(train_eval_partition_fn, 2)

            coder = coders.ExampleProtoCoder(records_metadata.schema)
            _ = train_dataset \
                | 'SerializeTrainExamples' >> beam.Map(coder.encode) \
                | 'ShuffleTraining' >> Shuffle() \
                | 'WriteTraining' >> beam.io.WriteToTFRecord(
                os.path.join(train_directory_path+"/train_data/", TRANSFORMED_TRAIN_DATA_FILE_PREFIX),
                file_name_suffix='.tfrecord.gz')

            _ = eval_dataset \
                | 'SerializeEvalExamples' >> beam.Map(coder.encode) \
                | 'ShuffleEval' >> Shuffle() \
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


@beam.ptransform_fn
def Shuffle(pcoll):  # pylint: disable=invalid-name
    import random
    return (pcoll
            | 'PairWithRandom' >> beam.Map(lambda x: (random.random(), x))
            | 'GroupByRandom' >> beam.GroupByKey()
            | 'DropRandom' >> beam.FlatMap(lambda (k, vs): vs))




# def partition_fn(user_id, partition_random_seed, percent_eval):
# I hope taxon_id will provide wide enough variation between results.
def partition_fn(ex, partition_random_seed, percent_eval):
    import hashlib
    m = hashlib.md5(str(ex["occurrence_id"]) + str(partition_random_seed))
    hash_value = int(m.hexdigest(), 16) % 100
    return 0 if hash_value >= percent_eval else 1

def make_preprocessing_fn(num_classes):
    import tensorflow_transform as tt
    """Creates a preprocessing function for reddit.
    Args:

    Returns:
      A preprocessing function.
    """

    def preprocessing_fn(i):

        # import tensorflow as tf

        m = {
            KEY_OCCURRENCE_ID: i[KEY_OCCURRENCE_ID],
            KEY_ELEVATION: tt.scale_to_0_1(i[KEY_ELEVATION]),
            KEY_AVG_TEMP: tt.scale_to_0_1(i[KEY_AVG_TEMP]),
            KEY_MIN_TEMP: tt.scale_to_0_1(i[KEY_MIN_TEMP]),
            KEY_MAX_TEMP: tt.scale_to_0_1(i[KEY_MAX_TEMP]),
            KEY_PRCP: tt.scale_to_0_1(i[KEY_PRCP]),
            KEY_DAYLIGHT: tt.scale_to_0_1(i[KEY_DAYLIGHT]),
            # KEY_GRID_ZONE: tt.hash_strings(i[KEY_GRID_ZONE], 1000)
            KEY_GRID_ZONE: i[KEY_GRID_ZONE],
            KEY_TAXON: i[KEY_TAXON]
        }

        # def relable_fn(v):
        #     print("called", v[0])
        #     if v == 0:
        #         return [0]
        #     else:
        #         return [1]

        # m[KEY_TAXON] = tf.cast(i[KEY_TAXON], tf.string)

        # if num_classes == 2:
        #     # m[KEY_TAXON] = tt.apply_function(relable_fn, i[KEY_TAXON])
        #     m[KEY_TAXON] = tf.cast(i[KEY_TAXON], tf.bool)
        #     m[KEY_TAXON] = tf.cast(m[KEY_TAXON], tf.int64)
        # else:
        #     m[KEY_TAXON] = i[KEY_TAXON]

        return m
        # m = {}
        # m[KEY_ELEVATION] = tt.scale_to_0_1(inputs[KEY_ELEVATION])
        # m[KEY_MAX_TEMP] = tt.scale_to_0_1(inputs[KEY_MAX_TEMP])
        # m[KEY_MIN_TEMP] = tt.scale_to_0_1(inputs[KEY_MIN_TEMP])
        # m[KEY_AVG_TEMP] = tt.scale_to_0_1(inputs[KEY_AVG_TEMP])
        # m[KEY_PRCP] = tt.scale_to_0_1(inputs[KEY_PRCP])
        # m[KEY_DAYLIGHT] = tt.scale_to_0_1(inputs[KEY_DAYLIGHT])
        #
        # m[KEY_GRID_ZONE] = tt.hash_strings(inputs[KEY_GRID_ZONE], 8)

        # m['tmax'] = array_ops.reshape(m['tmax'])

        # return m

    return preprocessing_fn

def make_input_schema(mode):
    from tensorflow_transform.tf_metadata import dataset_schema
    from tensorflow import FixedLenFeature, float32, string, int64, VarLenFeature
    from tensorflow.contrib.learn import ModeKeys
    """Input schema definition.
    Args:
      mode: tf.contrib.learn.ModeKeys specifying if the schema is being used for
        train/eval or prediction.
    Returns:
      A `Schema` object.
    """
    result = ({} if mode == ModeKeys.INFER else {
        KEY_TAXON: FixedLenFeature(shape=[], dtype=string)
    })
    result.update({
        KEY_OCCURRENCE_ID: FixedLenFeature(shape=[], dtype=string),
        KEY_ELEVATION: FixedLenFeature(shape=[1], dtype=float32),
        KEY_GRID_ZONE: FixedLenFeature(shape=[1], dtype=string),
        KEY_MAX_TEMP: FixedLenFeature(shape=[45], dtype=float32),
        KEY_MIN_TEMP: FixedLenFeature(shape=[45], dtype=float32),
        KEY_AVG_TEMP: FixedLenFeature(shape=[45], dtype=float32),
        KEY_PRCP: FixedLenFeature(shape=[45], dtype=float32),
        KEY_DAYLIGHT: FixedLenFeature(shape=[45], dtype=float32),
    })

    return dataset_schema.from_feature_spec(result)


    # features['elevation'].set_shape((1,))
    # features['label'].set_shape([1,1])

    # features['tmax'].set_shape([FLAGS.days_before_occurrence,1])
    # features['tmin'].set_shape([FLAGS.days_before_occurrence,1])
    # features['prcp'].set_shape([FLAGS.days_before_occurrence,1])
    # features['daylight'].set_shape([FLAGS.days_before_occurrence,1])
    #
    # # features['tmaxstacked'] = tf.reshape(features['tmax'], [9, 5])
    # tmax = tf.reduce_mean(tf.reshape(features['tmax'], [9, 5]), 1)
    # prcp = tf.reduce_mean(tf.reshape(features['prcp'], [9, 5]), 1)
    # daylight = tf.reduce_mean(tf.reshape(features['daylight'], [9, 5]), 1)
    #
    # x = tf.concat([tmax, prcp, daylight, features['elevation']], 0)


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    main()