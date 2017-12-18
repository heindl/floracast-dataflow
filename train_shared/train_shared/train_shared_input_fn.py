import tensorflow as tf
import os
from tensorflow_transform.tf_metadata import metadata_io
from tensorflow_transform.saved import input_fn_maker
from tensorflow import estimator
import train_shared_model
import constants


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


    # return transformed_input_fn

# def get_transformed_prediction_features(
#         tranformed_location,
#         raw_data_file_pattern,
#         batch_size,
#         mode
#     ):
#
#     transformed_metadata = metadata_io.read_metadata(
#         os.path.join(tranformed_location, "transformed_metadata"))
#
#     raw_metadata = metadata_io.read_metadata(
#         os.path.join(tranformed_location, "raw_metadata"))
#
#     transform_savedmodel_dir = os.path.join(tranformed_location, "transform_fn")
#
#     # raw_data_file_pattern=raw_data_file_pattern
#
#     raw_feature_spec = raw_metadata.schema.as_feature_spec()
#     raw_feature_keys = _prepare_feature_keys(raw_metadata, ["taxon"])
#     raw_training_feature_spec = {
#         key: raw_feature_spec[key]
#         for key in raw_feature_keys} # + raw_label_keys}
#
#     # transformed_feature_keys = _prepare_feature_keys(transformed_metadata, ["taxon"])
#
#     def raw_training_input_fn():
#         """Training input function that reads raw data and applies transforms."""
#
#         raw_data = tf.contrib.learn.io.read_batch_features(
#             file_pattern=raw_data_file_pattern,
#             batch_size=batch_size,
#             features=raw_training_feature_spec,
#             reader=gzip_reader_fn,
#             reader_num_threads=4,
#             queue_capacity=batch_size * 20,
#             randomize_input=(mode == estimator.ModeKeys.TRAIN),
#             num_epochs=(1 if mode != estimator.ModeKeys.TRAIN else None))
#
#         _, features = saved_transform_io.partially_apply_saved_transform(
#             transform_savedmodel_dir, raw_data)
#
#
#         features[KEY_MAX_TEMP] = tf.reduce_mean(tf.reshape(features[KEY_MAX_TEMP], [18, 5]), 1)
#
#         # transformed_features = {
#         #     k: v for k, v in six.iteritems(transformed_data)
#         #     if k in transformed_feature_keys}
#
#         # if convert_scalars_to_vectors:
#         #     transformed_features = _convert_scalars_to_vectors(transformed_features)
#
#         # if key_feature_name is not None:
#         #     transformed_features[key_feature_name] = keys
#
#         # if len(transformed_labels) == 1:
#         #     (_, transformed_labels), = transformed_labels.items()
#         return features #, transformed_labels
#
#     return raw_training_input_fn


# def get_transformed_reader_input_fn(transformed_metadata,
#                                     transformed_data_paths,
#                                     batch_size,
#                                     mode):
#
#     """Wrap the get input features function to provide the runtime arguments."""
#     return input_fn_maker.build_training_input_fn(
#         metadata=transformed_metadata,
#         file_pattern=(
#             transformed_data_paths[0] if len(transformed_data_paths) == 1
#             else transformed_data_paths),
#         training_batch_size=batch_size,
#         label_keys=['taxon'],
#         feature_keys=model.feature_keys(),
#         # key_feature_name='example_id',
#         reader=gzip_reader_fn,
#         # convert_scalars_to_vectors=False,
#         reader_num_threads=4,
#         queue_capacity=batch_size * 20,
#         randomize_input=(mode != estimator.ModeKeys.EVAL),
#         num_epochs=(1 if mode == estimator.ModeKeys.EVAL else None),
#         convert_scalars_to_vectors=False)


# def get_serving_input_fn(
#         args,
#         raw_label_keys,
#         raw_feature_keys=None
# ):
#     import tensorflow as tf
#     from tensorflow_transform.saved import saved_transform_io
#     # from tensorflow.contrib.learn.python.learn.utils.input_fn_utils import build_parsing_serving_input_fn
#     from tensorflow_transform.tf_metadata import metadata_io
#     import os
#
#     train_data_path=args.train_data_path
#
#     raw_metadata = metadata_io.read_metadata(os.path.join(train_data_path, "raw_metadata"))
#
#     raw_feature_spec = raw_metadata.schema.as_feature_spec()
#     raw_feature_keys = _prepare_feature_keys(raw_metadata,
#                                              raw_label_keys,
#                                              raw_feature_keys)
#     raw_serving_feature_spec = {key: raw_feature_spec[key] for key in raw_feature_keys}
#
#     def parsing_transforming_serving_input_fn():
#         """Serving input_fn that applies transforms to raw data in tf.Examples."""
#         # raw_input_fn = build_parsing_serving_input_fn(raw_serving_feature_spec)
#         raw_input_fn = tf.estimator.export.build_parsing_serving_input_receiver_fn(raw_serving_feature_spec)
#         # features, _, inputs = raw_input_fn()
#         features, receiver_tensors = raw_input_fn()
#
#         _, transformed_features = (
#             saved_transform_io.partially_apply_saved_transform(
#                 os.path.join(train_data_path, "transform_fn"), features))
#
#         # inputs['occurrence_id'] = tf.placeholder(dtype=tf.string, shape=[None])
#         # print("inputs", inputs)
#
#         # return tf.contrib.learn.InputFnOps(
#         #     transformed_features,
#         #     None,  # labels
#         #     inputs
#         # )
#         return tf.estimator.export.ServingInputReceiver(transformed_features, receiver_tensors)
#
#     return parsing_transforming_serving_input_fn
#
#
# def _prepare_feature_keys(metadata, label_keys, feature_keys=None):
#     """Infer feature keys if needed, and sanity-check label and feature keys."""
#     if label_keys is None:
#         raise ValueError("label_keys must be specified.")
#     if feature_keys is None:
#         feature_keys = list(
#             set(six.iterkeys(metadata.schema.column_schemas)) - set(label_keys))
#     overlap_keys = set(label_keys) & set(feature_keys)
#     if overlap_keys:
#         raise ValueError("Keys cannot be used as both a feature and a "
#                          "label: {}".format(overlap_keys))
#
#     return feature_keys