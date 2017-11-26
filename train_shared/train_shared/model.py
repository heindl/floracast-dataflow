import tensorflow as tf
import glob

def feature_columns():

    """Return the feature columns with their names and types."""

    # vocab_size = vocab_sizes[column_name]
    # column = tf.contrib.layers.sparse_column_with_integerized_feature(
    #     column_name, vocab_size, combiner='sum') // Sum means it's not reduced
    # embedding_size = int(math.floor(6 * vocab_size**0.25))
    # embedding = tf.contrib.layers.embedding_column(column,
    #                                                embedding_size,
    #                                                combiner='mean')

    # feature_columns = [
    #     tf.feature_column.numeric_column("x", shape=[28]),
    #     tf.contrib.layers.embedding_column(sparse_column_with_hash_bucket(
    #         column_name="grid",
    #         hash_bucket_size=1000
    #     ), dimension=8)
    # ]


    return [
        # tf.contrib.layers.embedding_column(tf.contrib.layers.sparse_column_with_hash_bucket(
        #     column_name="mgrs_grid_zone",
        #     hash_bucket_size=1000
        # ), dimension=8),
        tf.contrib.layers.real_valued_column("elevation", dtype=tf.float32),
        # tf.contrib.layers.real_valued_column("avg_temp", dtype=tf.float32),
        tf.contrib.layers.real_valued_column("max_temp", dimension=90, dtype=tf.float32),
        tf.contrib.layers.real_valued_column("min_temp", dimension=90, dtype=tf.float32),
        tf.contrib.layers.real_valued_column("precipitation", dimension=90, dtype=tf.float32),
        tf.contrib.layers.real_valued_column("daylight", dimension=90, dtype=tf.float32)
    ]

def feature_keys():
    return ["elevation", "max_temp", "min_temp", "precipitation", "daylight"]

def get_label_vocabularly(train_data_path):
    labels = []
    label_files = glob.glob(train_data_path + "/labels*")
    for file in label_files:
        with open(file, 'r') as label_file:
            taxa = label_file.read().splitlines()
            for t in taxa:
                labels.append(t)

    return labels

def get_estimator(args, run_config):

    def _get_model_fn(estimator):
        # def _model_fn(features, labels, mode):
        def _model_fn(features, labels, mode, config):
            if mode == tf.estimator.ModeKeys.PREDICT:
                key = features.pop('occurrence_id')
            # params = estimator.params
            model_fn_ops = estimator._model_fn(
                # features=features, labels=labels, mode=mode, params=params)
                features=features, labels=labels, mode=mode, config=config)
            if mode == tf.estimator.ModeKeys.PREDICT:
                model_fn_ops.predictions['occurrence_id'] = key
                # model_fn_ops.output_alternatives[None][1]['occurrence_id'] = key
            return model_fn_ops
        return _model_fn

    label_vocabulary = get_label_vocabularly(args.train_data_path)

    print("label vocabulary", label_vocabulary)

        # classifier = tf.contrib.learn.Estimator(
    return tf.estimator.Estimator(
        model_fn=_get_model_fn(
            # tf.contrib.learn.DNNClassifier(
            tf.estimator.DNNClassifier(
                feature_columns=feature_columns(),
                # hidden_units=args.hidden_units,
                hidden_units=[256, 128],
                n_classes=len(label_vocabulary),
                label_vocabulary=label_vocabulary,
                optimizer=tf.train.ProximalAdagradOptimizer(
                    learning_rate=0.01,
                    l1_regularization_strength=0.001
                ),
                config=run_config,
            )
        ),
        config=run_config,
    )