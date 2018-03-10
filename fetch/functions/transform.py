from tensorflow_transform.tf_metadata import dataset_schema, dataset_metadata
import tensorflow as tf
from os.path import isdir, join, split
from os import makedirs
import tensorflow_transform as tft
from tensorflow_transform.tf_metadata import metadata_io
from tensorflow_transform.saved import input_fn_maker, saved_transform_io
from tensorflow.contrib.learn.python.learn.utils import input_fn_utils
import constants
from math import ceil
from tfrecords import OccurrenceTFRecords
import functools
import shutil
import six
import apache_beam as beam
from google.cloud import storage
from datetime import datetime
from tensorflow_transform import coders

TRANSFORMED_RAW_METADATA_PATH = "raw_metadata"
TRANSFORMED_METADATA_PATH = "transformed_metadata"
TRANSFORM_FN_PATH = "transform_fn"

class FetchExampleFiles(beam.DoFn):
    def __init__(self, project, bucket):
        super(FetchExampleFiles, self).__init__()
        self._project = project
        self._bucket = bucket

    def process(self, i):

        client = storage.client.Client(project=self._project)
        bucket = client.get_bucket(self._bucket)

        prefix = "gs://"+self._bucket

        files = {}
        for b in bucket.list_blobs(prefix="occurrences", fields="items/name"):
            p, n = split(b.name)
            if not p or not n or p == "/" or n == "/":
                continue
            if p in files:
                files[p] = n if n > files[p] else files[p]
            else:
                files[p] = n

        for b in bucket.list_blobs(prefix="protected_areas", fields="items/name"):
            p, n = split(b.name)
            if not p or not n or p == "/" or n == "/":
                continue
            if p in files:
                files[p] = n if n > files[p] else files[p]
            else:
                files[p] = n

        for i in files:
            yield join(prefix, i, files[i])

        random_paths = []
        randoms = {}
        for b in bucket.list_blobs(prefix="random", fields="items/name"):
            p, n = split(b.name)
            if not p or not n or p == "/" or n == "/":
                continue
            if p in random_paths:
                randoms[p].append(n)
            else:
                random_paths.append(p)
                randoms[p] = [n]

        random_paths = sorted(random_paths, reverse=True)
        if len(random_paths) == 0:
            return

        for f in randoms[random_paths[0]]:
            yield join(prefix, random_paths[0], f)


class TransformData:

    def __init__(self, bucket="floracast-datamining"):

        self.output_path = "gs://" + bucket + "/transformers/" + datetime.now().strftime("%s")

        self.raw_metadata_path = join(self.output_path, TRANSFORMED_RAW_METADATA_PATH)
        self.transformed_metadata_path = join(self.output_path, TRANSFORMED_METADATA_PATH)
        self.transform_fn_path = join(self.output_path, TRANSFORM_FN_PATH)
        self.coder = coders.ExampleProtoCoder(self.create_raw_metadata(tf.estimator.ModeKeys.TRAIN).schema)

    @staticmethod
    def make_preprocessing_fn():

        def preprocessing_fn(i):

            r = {}

            # Identifiable
            # r[constants.KEY_CATEGORY] = tft.string_to_int(i[constants.KEY_CATEGORY])
            r[constants.KEY_CATEGORY] = i[constants.KEY_CATEGORY]
            # Hopefully this will bucket Random and NameUsage without the necessity for boolean conversion.

            # Categorical
            r[constants.KEY_S2_TOKENS] = tft.string_to_int(i[constants.KEY_S2_TOKENS], default_value=0)
            r[constants.KEY_ECO_BIOME] = tft.string_to_int(i[constants.KEY_ECO_BIOME], default_value=0)
            r[constants.KEY_ECO_NUM] = tft.string_to_int(i[constants.KEY_ECO_NUM], default_value=0)

            # Ordinal
            r[constants.KEY_ELEVATION] = tft.scale_to_0_1(i[constants.KEY_ELEVATION])
            r[constants.KEY_MIN_TEMP] = tft.scale_to_0_1(i[constants.KEY_MIN_TEMP])
            r[constants.KEY_MAX_TEMP] = tft.scale_to_0_1(i[constants.KEY_MAX_TEMP])
            r[constants.KEY_DAYLIGHT] = tft.scale_to_0_1(i[constants.KEY_DAYLIGHT])
            r[constants.KEY_PRCP] = tft.scale_to_0_1(i[constants.KEY_PRCP])

            return r

        return preprocessing_fn

    @staticmethod
    def create_raw_metadata(mode):

        """Input schema definition.
        Args:
          mode: tf.contrib.learn.ModeKeys specifying if the schema is being used for
            train/eval or prediction.
        Returns:
          A `Schema` object.
        """
        result = ({} if mode == tf.estimator.ModeKeys.PREDICT else {
            constants.KEY_CATEGORY: tf.FixedLenFeature(shape=[], dtype=tf.string), # Equivalent of KEY_TAXON
            # constants.KEY_EXAMPLE_ID: FixedLenFeature(shape=[], dtype=string),
        })
        result.update({
            constants.KEY_S2_TOKENS: tf.FixedLenFeature(shape=[8], dtype=tf.string),
            constants.KEY_ECO_BIOME: tf.FixedLenFeature(shape=[], dtype=tf.string),
            constants.KEY_ECO_NUM: tf.FixedLenFeature(shape=[], dtype=tf.string),
            constants.KEY_ELEVATION: tf.FixedLenFeature(shape=[], dtype=tf.int64),
            constants.KEY_MAX_TEMP: tf.FixedLenFeature(shape=[90], dtype=tf.float32),
            constants.KEY_MIN_TEMP: tf.FixedLenFeature(shape=[90], dtype=tf.float32),
            constants.KEY_AVG_TEMP: tf.FixedLenFeature(shape=[90], dtype=tf.float32),
            constants.KEY_PRCP: tf.FixedLenFeature(shape=[90], dtype=tf.float32),
            constants.KEY_DAYLIGHT: tf.FixedLenFeature(shape=[90], dtype=tf.float32),
        })

        return dataset_metadata.DatasetMetadata(schema=dataset_schema.from_feature_spec(result))


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

class TrainingData:

    def __init__(self, transform_data_path, model_path, train_batch_size, train_epochs):

        self._train_batch_size = train_batch_size
        self._train_epochs = train_epochs

        if transform_data_path == "":
            raise ValueError("A path with transformed training data is required")

        if not model_path.startswith("/tmp"):
            raise ValueError("Invalid model directory path")

        self._model_path = model_path
        if isdir(self._model_path):
            # Clean up existing model dir.
            shutil.rmtree(self._model_path)

        makedirs(self._model_path)

        self._raw_metadata_path = join(transform_data_path, TRANSFORMED_RAW_METADATA_PATH)
        self._transformed_metadata_path = join(transform_data_path, TRANSFORMED_METADATA_PATH)
        self._transform_fn_path = join(transform_data_path, TRANSFORM_FN_PATH)

        for dir in [
            self._raw_metadata_path,
            self._transformed_metadata_path,
            self._transform_fn_path,
        ]:
            if not isdir(dir):
                raise ValueError("Directory doesn't exist: ", dir)

        self._tfrecord_parser = OccurrenceTFRecords(self._example_path + "/*.gz")

    def _feature_columns(self):
        meta_data = metadata_io.read_metadata(self._transformed_metadata_path)

        eco_num_buckets = meta_data.schema[constants.KEY_ECO_NUM].domain.max_value
        eco_biome_buckets = meta_data.schema[constants.KEY_ECO_BIOME].domain.max_value
        s2_token_buckets = meta_data.schema[constants.KEY_S2_TOKENS].domain.max_value

        eco_num_column = tf.feature_column.categorical_column_with_identity(
            constants.KEY_ECO_NUM,
            num_buckets=eco_num_buckets,
            default_value=0
        )

        eco_biome_column = tf.feature_column.categorical_column_with_identity(
            constants.KEY_ECO_BIOME,
            num_buckets=eco_biome_buckets,
            default_value=0
        )

        s2_token_column = tf.feature_column.categorical_column_with_identity(
            constants.KEY_S2_TOKENS,
            num_buckets=s2_token_buckets,
            default_value=0
        )

        eco_num_embedding_dimensions = ceil(eco_num_buckets ** 0.25)
        s2_token_embedding_dimensions = ceil(s2_token_buckets ** 0.25)

        return [
            tf.feature_column.numeric_column(constants.KEY_ELEVATION, shape=[]),
            tf.feature_column.numeric_column(constants.KEY_MAX_TEMP, shape=[8]),
            tf.feature_column.numeric_column(constants.KEY_MIN_TEMP, shape=[8]),
            tf.feature_column.numeric_column(constants.KEY_PRCP, shape=[8]),
            tf.feature_column.numeric_column(constants.KEY_DAYLIGHT, shape=[8]),
            # Indicator column for eco_biome because there are only 9 categories.
            tf.feature_column.indicator_column(eco_biome_column),
            tf.feature_column.embedding_column(eco_num_column, dimension=eco_num_embedding_dimensions),
            tf.feature_column.embedding_column(s2_token_column, dimension=s2_token_embedding_dimensions),
        ]

    def _reshape_weather(self, f):
        f = tf.reshape(f, [18, 5])
        f = tf.reduce_mean(f, 1)
        f = tf.slice(f, [10], [8])
        return f

    def _all_feature_keys(self):
        return self._weather_keys() + [
            constants.KEY_ELEVATION,
            constants.KEY_ECO_NUM,
            constants.KEY_ECO_BIOME,
            constants.KEY_S2_TOKENS,
        ]

    def _weather_keys(self):
        return [
            constants.KEY_MAX_TEMP,
            constants.KEY_MIN_TEMP,
            constants.KEY_PRCP,
            constants.KEY_DAYLIGHT,
        ]

    # def get_label_vocabularly(train_data_path):
    #     labels = []
    #     label_files = glob.glob(train_data_path + "/labels*")
    #     for file in label_files:
    #         with open(file, 'r') as label_file:
    #             taxa = label_file.read().splitlines()
    #             for t in taxa:
    #                 labels.append(t)
    #
    #     return labels

    def _gzip_reader_fn(self):
        return tf.TFRecordReader(
            options=tf.python_io.TFRecordOptions(
                compression_type=tf.python_io.TFRecordCompressionType.GZIP,
            )
        )

    def input_functions(self, percentage_split):

        eval_file, train_file = self._tfrecord_parser.train_test_split(percentage_split)

        eval_batch_size, _ = self._tfrecord_parser.count(eval_file)

        print("calling eval input function", eval_file, eval_batch_size)
        print("calling train input function", train_file, self._train_batch_size, self._train_epochs)


        train_fn = functools.partial(self._transformed_input_fn,
                                   raw_data_file_pattern=train_file,
                                   mode=tf.estimator.ModeKeys.TRAIN,
                                   batch_size=self._train_batch_size,
                                   epochs=self._train_epochs)

        eval_fn = functools.partial(self._transformed_input_fn,
                                    raw_data_file_pattern=eval_file,
                                    mode=tf.estimator.ModeKeys.EVAL,
                                    batch_size=eval_batch_size,
                                    epochs=1)

        return eval_fn, train_fn

    def _raw_metadata(self):
        return metadata_io.read_metadata(self._raw_metadata_path)

    def _transformed_metadata(self):
        return metadata_io.read_metadata(self._transformed_metadata_path)

    def _transformed_input_fn(self, raw_data_file_pattern, mode, batch_size, epochs):

        labels = ([constants.KEY_EXAMPLE_ID] if mode == tf.estimator.ModeKeys.PREDICT else [constants.KEY_CATEGORY])

        fn = input_fn_maker.build_transforming_training_input_fn(
            raw_metadata=self._raw_metadata(),
            transformed_metadata=self._transformed_metadata(),
            transform_savedmodel_dir=self._transform_fn_path,
            raw_data_file_pattern=raw_data_file_pattern,
            training_batch_size=batch_size,
            # transformed_label_keys=constants.KEY_CATEGORY,
            transformed_label_keys=labels,
            transformed_feature_keys=self._all_feature_keys(),
            key_feature_name=None,
            convert_scalars_to_vectors=True,
            # Read batch features.
            reader=self._gzip_reader_fn,
            # num_epochs=(1 if mode != estimator.ModeKeys.TRAIN else None),
            num_epochs=epochs,
            randomize_input=True,
            # randomize_input=(mode == estimator.ModeKeys.TRAIN),
            queue_capacity=batch_size * 20,
        )

        features, labels = fn()
        labels = tf.reshape(labels, [-1])
        labels = tf.not_equal(labels, "random")

        # if mode == tf.estimator.ModeKeys.EVAL:
        #     labels = tf.Print(labels, [labels])

        for weather_key in self._weather_keys():
            features[weather_key] = tf.map_fn(self._reshape_weather, features[weather_key])

        if mode == tf.estimator.ModeKeys.PREDICT:
            # features[constants.KEY_OCCURRENCE_ID] = labels
            return features
        else:
            return features, labels

    def _convert_scalars_to_vectors(self, features):
        """Vectorize scalar columns to meet FeatureColumns input requirements."""
        def maybe_expand_dims(tensor):
            # Ignore the SparseTensor case.  In principle it's possible to have a
            # rank-1 SparseTensor that needs to be expanded, but this is very
            # unlikely.
            if isinstance(tensor, tf.Tensor) and tensor.get_shape().ndims == 1:
                tensor = tf.expand_dims(tensor, -1)
            return tensor

        return {name: maybe_expand_dims(tensor)
        for name, tensor in six.iteritems(features)}

    def _serving_input_receiver_fn(self):

        raw_feature_spec = self._raw_metadata().schema.as_feature_spec()

        # Exclude label keys and other unnecessary features.
        # This is typically used to specify the raw labels and weights,
        # so that transformations involving these do not pollute the serving graph.
        all_raw_feature_keys = six.iterkeys(self._raw_metadata().schema.column_schemas)
        raw_feature_keys = list(set(all_raw_feature_keys) - set(constants.KEY_CATEGORY))

        raw_serving_feature_spec = {key: raw_feature_spec[key]
                                    for key in raw_feature_keys}

        def parsing_transforming_serving_input_receiver_fn():
            """Serving input_fn that applies transforms to raw data in tf.Examples."""
            raw_input_fn = input_fn_utils.build_parsing_serving_input_fn(
                raw_serving_feature_spec, default_batch_size=None)
            raw_features, _, inputs = raw_input_fn()
            _, transformed_features = (
                saved_transform_io.partially_apply_saved_transform(
                    self._transform_fn_path, raw_features))

            # Convert scalars to vectors
            transformed_features = self._convert_scalars_to_vectors(transformed_features)

            for key in self._weather_keys():
                transformed_features[key] = tf.map_fn(self._reshape_weather, transformed_features[key])

            return tf.estimator.export.ServingInputReceiver(
                transformed_features, inputs)

        return parsing_transforming_serving_input_receiver_fn

    def export_model(self):
        model = self.get_estimator()
        export_dir = "/tmp/"
        model.export_savedmodel(
            export_dir_base=join(self._model_path, "exports/"),
            serving_input_receiver_fn=self._serving_input_receiver_fn()
        )


    def get_estimator(self):

        # run_config = tf.estimator.RunConfig(model_dir=FLAGS.model_dir)

       return tf.estimator.DNNClassifier(
            feature_columns=self._feature_columns(),
            hidden_units=[100, 75, 50, 25],
            # optimizer=tf.train.ProximalAdagradOptimizer(
            #     learning_rate=0.01,
            #     l1_regularization_strength=0.001
            # ),
            model_dir=self._model_path,
        )

# def get_estimator(run_config, feature_columns):
#
#     def _get_model_fn(estimator):
#         # def _model_fn(features, labels, mode):
#         def _model_fn(features, labels, mode, config):
#             if mode == tf.estimator.ModeKeys.PREDICT:
#                 key = features.pop(constants.KEY_EXAMPLE_ID)
#             # params = estimator.params
#             model_fn_ops = estimator._model_fn(
#                 # features=features, labels=labels, mode=mode, params=params)
#                 features=features, labels=labels, mode=mode, config=config)
#             if mode == tf.estimator.ModeKeys.PREDICT:
#                 model_fn_ops.predictions[constants.KEY_EXAMPLE_ID] = key
#                 # model_fn_ops.output_alternatives[None][1]['occurrence_id'] = key
#             return model_fn_ops
#         return _model_fn
#
#         # classifier = tf.contrib.learn.Estimator(
#     return tf.estimator.Estimator(
#         model_fn=_get_model_fn(
#             # tf.contrib.learn.DNNClassifier(
#             tf.estimator.DNNClassifier(
#                 feature_columns=feature_columns,
#                 hidden_units=[100, 75, 50, 25],
#                 # optimizer=tf.train.ProximalAdagradOptimizer(
#                 #     learning_rate=0.01,
#                 #     l1_regularization_strength=0.001
#                 # ),
#             )
#         ),
#         config=run_config,
#     )