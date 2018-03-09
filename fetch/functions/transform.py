import tensorflow as tf
from tensorflow_transform.tf_metadata import dataset_schema, dataset_metadata
from tensorflow import FixedLenFeature, float32, int64, string
from tensorflow.contrib.learn import ModeKeys
import os
import tensorflow_transform as tft
from tensorflow_transform.tf_metadata import metadata_io
from tensorflow_transform.saved import input_fn_maker
from tensorflow import estimator
import constants
from math import ceil
from tfrecords import TFRecordParser
import functools
import shutil

def make_preprocessing_fn():

    def preprocessing_fn(i):

        r = {}

        # Identifiable
        r[constants.KEY_CATEGORY] = tft.string_to_int(i[constants.KEY_CATEGORY])
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


def create_raw_metadata(mode):

    """Input schema definition.
    Args:
      mode: tf.contrib.learn.ModeKeys specifying if the schema is being used for
        train/eval or prediction.
    Returns:
      A `Schema` object.
    """
    result = ({} if mode == ModeKeys.INFER else {
        constants.KEY_CATEGORY: FixedLenFeature(shape=[], dtype=string), # Equivalent of KEY_TAXON
        # constants.KEY_EXAMPLE_ID: FixedLenFeature(shape=[], dtype=string),
    })
    result.update({
        constants.KEY_S2_TOKENS: FixedLenFeature(shape=[8], dtype=string),
        constants.KEY_ECO_BIOME: FixedLenFeature(shape=[], dtype=string),
        constants.KEY_ECO_NUM: FixedLenFeature(shape=[], dtype=string),
        constants.KEY_ELEVATION: FixedLenFeature(shape=[], dtype=int64),
        constants.KEY_MAX_TEMP: FixedLenFeature(shape=[90], dtype=float32),
        constants.KEY_MIN_TEMP: FixedLenFeature(shape=[90], dtype=float32),
        constants.KEY_AVG_TEMP: FixedLenFeature(shape=[90], dtype=float32),
        constants.KEY_PRCP: FixedLenFeature(shape=[90], dtype=float32),
        constants.KEY_DAYLIGHT: FixedLenFeature(shape=[90], dtype=float32),
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

    def __init__(self, train_data_path, model_path, train_batch_size, train_epochs):

        self._train_batch_size = train_batch_size
        self._train_epochs = train_epochs

        if train_data_path == "":
            raise ValueError("A path with transformed training data is required")

        if not model_path.startswith("/tmp"):
            raise ValueError("Invalid model directory path")

        self._model_path = model_path
        if os.path.isdir(self._model_path):
            # Clean up existing model dir.
            shutil.rmtree(self._model_path)

        os.makedirs(self._model_path)

        self._example_path = os.path.join(train_data_path, "examples")
        self._raw_metadata_path = os.path.join(train_data_path, "raw_metadata")
        self._transformed_metadata_path = os.path.join(train_data_path, "transformed_metadata")
        self._transform_fn_path = os.path.join(train_data_path, "transform_fn")

        for dir in [
            self._example_path,
            self._raw_metadata_path,
            self._transformed_metadata_path,
            self._transform_fn_path,
        ]:
            if not os.path.isdir(dir):
                raise ValueError("Directory doesn't exist: ", dir)

        self._tfrecord_parser = TFRecordParser(self._example_path+"/*.gz")

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

    def _transformed_input_fn(self, raw_data_file_pattern, mode, batch_size, epochs):

        raw_metadata = metadata_io.read_metadata(self._raw_metadata_path)
        transformed_metadata=metadata_io.read_metadata(self._transformed_metadata_path)

        labels = ([constants.KEY_EXAMPLE_ID] if mode == estimator.ModeKeys.PREDICT else [constants.KEY_CATEGORY])

        fn = input_fn_maker.build_transforming_training_input_fn(
            raw_metadata=raw_metadata,
            transformed_metadata=transformed_metadata,
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
            randomize_input=(mode == estimator.ModeKeys.TRAIN),
            queue_capacity=batch_size * 20,
        )

        features, labels = fn()
        labels = tf.reshape(labels, [-1])

        if mode == estimator.ModeKeys.EVAL:
            labels = tf.Print(labels, [labels])

        for label in self._weather_keys():
            features[label] = tf.map_fn(self._reshape_weather, features[label])

        if mode == estimator.ModeKeys.PREDICT:
            # features[constants.KEY_OCCURRENCE_ID] = labels
            return features
        else:
            # TODO: This is obviously a problem because we don't know if 0 is occurrence or random
            # return features, tf.not_equal(labels, "0")

            return features, labels


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