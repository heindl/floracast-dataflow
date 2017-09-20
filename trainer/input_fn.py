
def gzip_reader_fn():
    import tensorflow as tf
    return tf.TFRecordReader(options=tf.python_io.TFRecordOptions(
        compression_type=tf.python_io.TFRecordCompressionType.GZIP))

def get_test_prediction_data_fn(
        args,
        raw_data_file_pattern,
):
    from tensorflow_transform.saved import input_fn_maker
    import os
    from tensorflow_transform.tf_metadata import metadata_io

    train_data_path = args['train_data_path']

    transformed_metadata = metadata_io.read_metadata(
        os.path.join(train_data_path, "transformed_metadata"))

    raw_metadata = metadata_io.read_metadata(
        os.path.join(train_data_path, "raw_metadata"))

    tranform_model = os.path.join(train_data_path, "transform_fn")

    return input_fn_maker.build_transforming_training_input_fn(
        raw_metadata=raw_metadata,
        transformed_metadata=transformed_metadata,
        transform_savedmodel_dir=tranform_model,
        raw_data_file_pattern=raw_data_file_pattern,
        training_batch_size=1,
        raw_label_keys=['taxon'],
        transformed_label_keys=['taxon'],
        # raw_feature_keys=feature_keys(),
        # transformed_feature_keys=feature_keys(),
        # key_feature_name='occurrence_id',
        convert_scalars_to_vectors=False,
        num_epochs=1,
    )


def get_transformed_reader_input_fn(transformed_metadata,
                                    transformed_data_paths,
                                    batch_size,
                                    mode):
    from tensorflow_transform.saved import input_fn_maker
    from tensorflow import estimator
    import model

    """Wrap the get input features function to provide the runtime arguments."""
    return input_fn_maker.build_training_input_fn(
        metadata=transformed_metadata,
        file_pattern=(
            transformed_data_paths[0] if len(transformed_data_paths) == 1
            else transformed_data_paths),
        training_batch_size=batch_size,
        label_keys=['taxon'],
        feature_keys=model.feature_keys(),
        # key_feature_name='example_id',
        reader=gzip_reader_fn,
        # convert_scalars_to_vectors=False,
        reader_num_threads=4,
        queue_capacity=batch_size * 20,
        randomize_input=(mode != estimator.ModeKeys.EVAL),
        num_epochs=(1 if mode == estimator.ModeKeys.EVAL else None),
        convert_scalars_to_vectors=False)


def get_serving_input_fn(
        args,
        raw_label_keys,
        raw_feature_keys=None
):
    import tensorflow as tf
    from tensorflow_transform.saved import saved_transform_io
    # from tensorflow.contrib.learn.python.learn.utils.input_fn_utils import build_parsing_serving_input_fn
    from tensorflow_transform.tf_metadata import metadata_io
    import os

    train_data_path=args.train_data_path

    raw_metadata = metadata_io.read_metadata(os.path.join(train_data_path, "raw_metadata"))

    raw_feature_spec = raw_metadata.schema.as_feature_spec()
    raw_feature_keys = _prepare_feature_keys(raw_metadata,
                                             raw_label_keys,
                                             raw_feature_keys)
    raw_serving_feature_spec = {key: raw_feature_spec[key] for key in raw_feature_keys}

    def parsing_transforming_serving_input_fn():
        """Serving input_fn that applies transforms to raw data in tf.Examples."""
        # raw_input_fn = build_parsing_serving_input_fn(raw_serving_feature_spec)
        raw_input_fn = tf.estimator.export.build_parsing_serving_input_receiver_fn(raw_serving_feature_spec)
        # features, _, inputs = raw_input_fn()
        features, receiver_tensors = raw_input_fn()
        _, transformed_features = (
            saved_transform_io.partially_apply_saved_transform(
                os.path.join(train_data_path, "transform_fn"), features))
        # inputs['occurrence_id'] = tf.placeholder(dtype=tf.string, shape=[None])
        # print("inputs", inputs)

        # return tf.contrib.learn.InputFnOps(
        #     transformed_features,
        #     None,  # labels
        #     inputs
        # )
        return tf.estimator.export.ServingInputReceiver(transformed_features, receiver_tensors)

    return parsing_transforming_serving_input_fn


def _prepare_feature_keys(metadata, label_keys, feature_keys=None):
    import six
    """Infer feature keys if needed, and sanity-check label and feature keys."""
    if label_keys is None:
        raise ValueError("label_keys must be specified.")
    if feature_keys is None:
        feature_keys = list(
            set(six.iterkeys(metadata.schema.column_schemas)) - set(label_keys))
    overlap_keys = set(label_keys) & set(feature_keys)
    if overlap_keys:
        raise ValueError("Keys cannot be used as both a feature and a "
                         "label: {}".format(overlap_keys))

    return feature_keys