import tensorflow as tf
import glob
import constants
import tensorflow as tf
import os
from tensorflow_transform.tf_metadata import metadata_io
from tensorflow_transform.saved import input_fn_maker
from tensorflow import estimator
import train_shared_model
import constants


def normalizer(f):
    f = tf.reshape(f, [18, 5])
    f = tf.reduce_mean(f, 1)
    f = tf.slice(f, [10], [8])
    return f

def feature_columns():

    return [
        tf.feature_column.numeric_column(constants.KEY_ELEVATION, shape=[1]),
        tf.feature_column.numeric_column(constants.KEY_MAX_TEMP, shape=[8]),
        tf.feature_column.numeric_column(constants.KEY_MIN_TEMP, shape=[8]),
        tf.feature_column.numeric_column(constants.KEY_PRCP, shape=[8]),
        tf.feature_column.numeric_column(constants.KEY_DAYLIGHT, shape=[8]),
        tf.feature_column.numeric_column(constants.KEY_GRID_ZONE, shape=[1])
    ]

def all_feature_keys():
    return list_feature_keys() + [constants.KEY_ELEVATION, constants.KEY_GRID_ZONE]


def list_feature_keys():
    return [constants.KEY_MAX_TEMP, constants.KEY_MIN_TEMP, constants.KEY_PRCP, constants.KEY_DAYLIGHT]
    # return ["elevation", "max_temp", "min_temp", "precipitation", "daylight"]

def get_label_vocabularly(train_data_path):
    labels = []
    label_files = glob.glob(train_data_path + "/labels*")
    for file in label_files:
        with open(file, 'r') as label_file:
            taxa = label_file.read().splitlines()
            for t in taxa:
                labels.append(t)

    return labels

def gzip_reader_fn():
    return tf.TFRecordReader(options=tf.python_io.TFRecordOptions(
        compression_type=tf.python_io.TFRecordCompressionType.GZIP))

def transformed_input_fn(transformed_location, batch_size, mode, epochs):

    def map(f):
        f = tf.reshape(f, [18, 5])
        f = tf.reduce_mean(f, 1)
        f = tf.slice(f, [10], [8])
        return f

    raw_data_file_pattern = ""
    if mode == estimator.ModeKeys.EVAL:
        raw_data_file_pattern = transformed_location + "/eval_data/*.gz"
    if mode == estimator.ModeKeys.TRAIN:
        raw_data_file_pattern = transformed_location + "/train_data/*.gz"

    fn = input_fn_maker.build_transforming_training_input_fn(
        raw_metadata=metadata_io.read_metadata(os.path.join(transformed_location, "raw_metadata")),
        transformed_metadata=metadata_io.read_metadata(os.path.join(transformed_location, "transformed_metadata")),
        transform_savedmodel_dir=os.path.join(transformed_location, "transform_fn"),
        raw_data_file_pattern=raw_data_file_pattern,
        training_batch_size=batch_size,
        transformed_label_keys=[constants.KEY_TAXON],
        transformed_feature_keys=train_shared_model.all_feature_keys(),
        key_feature_name=None,
        convert_scalars_to_vectors=True,
        # Read batch features.
        reader=gzip_reader_fn,
        # num_epochs=(1 if mode != estimator.ModeKeys.TRAIN else None),
        num_epochs=epochs,
        randomize_input=(mode == estimator.ModeKeys.TRAIN),
        queue_capacity=batch_size * 20,
    )

    features, labels = fn()

    labels = tf.reshape(labels, [-1])

    features[constants.KEY_GRID_ZONE] = tf.string_to_number(features[constants.KEY_GRID_ZONE], out_type=tf.int32)

    for label in train_shared_model.list_feature_keys():
        features[label] = tf.map_fn(map, features[label])

    if mode == estimator.ModeKeys.PREDICT:
        return features
    else:
        return features, tf.not_equal(labels, '0')


def get_estimator(args, run_config):
    return tf.estimator.DNNClassifier(
        feature_columns=feature_columns(),
        # hidden_units=args.hidden_units,
        hidden_units=[10, 20, 10],
        # hidden_units=[64, 32],
        # n_classes=len(label_vocabulary),
        # label_vocabulary=label_vocabulary,
        # optimizer=tf.train.ProximalAdagradOptimizer(
        #     learning_rate=0.01,
        #     l1_regularization_strength=0.001
        # ),
        config=run_config,
    )

# def get_estimator(args, run_config):
#
#     def _get_model_fn(estimator):
#         # def _model_fn(features, labels, mode):
#         def _model_fn(features, labels, mode, config):
#             if mode == tf.estimator.ModeKeys.PREDICT:
#                 key = features.pop('occurrence_id')
#             # params = estimator.params
#             model_fn_ops = estimator._model_fn(
#                 # features=features, labels=labels, mode=mode, params=params)
#                 features=features, labels=labels, mode=mode, config=config)
#             if mode == tf.estimator.ModeKeys.PREDICT:
#                 model_fn_ops.predictions['occurrence_id'] = key
#                 # model_fn_ops.output_alternatives[None][1]['occurrence_id'] = key
#             return model_fn_ops
#         return _model_fn
#
#     label_vocabulary = get_label_vocabularly(args.train_data_path)
#
#     print("label vocabulary", label_vocabulary)
#
#         # classifier = tf.contrib.learn.Estimator(
#     return tf.estimator.Estimator(
#         model_fn=_get_model_fn(
#             # tf.contrib.learn.DNNClassifier(
#             tf.estimator.DNNClassifier(
#                 feature_columns=feature_columns(),
#                 # hidden_units=args.hidden_units,
#                 hidden_units=[256, 128],
#                 n_classes=len(label_vocabulary),
#                 label_vocabulary=label_vocabulary,
#                 # optimizer=tf.train.ProximalAdagradOptimizer(
#                 #     learning_rate=0.01,
#                 #     l1_regularization_strength=0.001
#                 # ),
#                 config=run_config,
#             )
#         ),
#         config=run_config,
#     )