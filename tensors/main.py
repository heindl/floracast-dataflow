from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
import traceback
import tensorflow as tf
from tensorflow.contrib.layers import real_valued_column, sparse_column_with_hash_bucket
from tensorflow.contrib.learn import Experiment, MetricSpec, ModeKeys, learn_runner, DNNClassifier
from tensorflow.contrib.learn.python.learn.estimators import constants
from tensorflow.contrib.learn.python.learn.estimators import rnn_common
from tensorflow.contrib.learn.python.learn.estimators import dynamic_rnn_estimator
from tensorflow.contrib.learn.python.learn.estimators import run_config as run_config_lib
# from tensorflow.contrib.learn.python.learn import learn_runner
import random
import tempfile


flags = tf.app.flags
FLAGS = flags.FLAGS

flags.DEFINE_string("model_dir", "", "Base directory for output models.")

flags.DEFINE_integer(
    "epochs",
    50,
    "Number of epochs to run over file")

flags.DEFINE_integer(
    "train_steps",
    200,
    "Number of training steps.")

flags.DEFINE_integer(
    "training_batch_size",
    40,
    "Size of training batch")

flags.DEFINE_integer(
    "days_before_occurrence",
    45,
    "Size of training batch")

flags.DEFINE_string(
    "train_data",
    "./train.tfrecords",
    "Path to the training data.")

flags.DEFINE_string(
    "eval_data",
    "./eval.tfrecords",
    "Path to the eval data.")

def parse_file(file, mode):
    num_epochs = FLAGS.epochs
    if mode == ModeKeys.EVAL:
        num_epochs = 1

    filename_queue = tf.train.string_input_producer([file], num_epochs=num_epochs)

    reader = tf.TFRecordReader()
    _, example = reader.read(filename_queue)

    context_parsed, sequence_parsed = tf.parse_single_sequence_example(
        example,
        context_features={
            'elevation': tf.FixedLenFeature(shape=[1], dtype=tf.float32),
            'grid-zone': tf.VarLenFeature(dtype=tf.string),
            'label': tf.FixedLenFeature(shape=[], dtype=tf.int64)
        },
        sequence_features={
            "tmax": tf.FixedLenSequenceFeature(shape=[1], dtype=tf.float32),
            "tmin": tf.FixedLenSequenceFeature(shape=[1], dtype=tf.float32),
            "prcp": tf.FixedLenSequenceFeature(shape=[1], dtype=tf.float32),
            "daylight": tf.FixedLenSequenceFeature(shape=[1], dtype=tf.float32),
        }
    )

    features = context_parsed.copy()
    features.update(sequence_parsed)

    # features['elevation'].set_shape((1,))
    # features['label'].set_shape([1,1])

    features['tmax'].set_shape([FLAGS.days_before_occurrence,1])
    features['tmin'].set_shape([FLAGS.days_before_occurrence,1])
    features['prcp'].set_shape([FLAGS.days_before_occurrence,1])
    features['daylight'].set_shape([FLAGS.days_before_occurrence,1])

    # features['tmaxstacked'] = tf.reshape(features['tmax'], [9, 5])
    tmax = tf.reduce_mean(tf.reshape(features['tmax'], [9, 5]), 1)
    prcp = tf.reduce_mean(tf.reshape(features['prcp'], [9, 5]), 1)
    daylight = tf.reduce_mean(tf.reshape(features['daylight'], [9, 5]), 1)

    x = tf.concat([tmax, prcp, daylight, features['elevation']], 0)

    return {
        "x": x,
        "grid": features["grid-zone"],
        "label": features["label"]
    }


def get_input_fn(file, mode, batch_size):
    def _input_fn():

        features = parse_file(file, mode)

        seed = random.seed()

        x = tf.train.shuffle_batch(
            features,
            # allow_smaller_final_batch=True,
            batch_size=batch_size,
            seed=seed,
            capacity=3000,
            min_after_dequeue=1000,
        #     min_after_dequeue = 10000
        # capacity = min_after_dequeue + 3 * batch_size
            name="read_batch_features_{}".format(mode)
        )

        x['x'].set_shape((batch_size, 28))

        x['x'] = tf.contrib.layers.batch_norm(
            inputs=x['x'],
            is_training=(mode == ModeKeys.TRAIN),
            center=False,
            scale=False,
        )
        # x["elevation"] = tf.reshape(x["elevation"], [-1])
        #
        # x['tmax'] = tf.contrib.layers.batch_norm(
        #     inputs=x['tmax'],
        #     is_training=True,
        #     center=True,
        #     scale=True,
        # )
        # x['prcp'] = tf.contrib.layers.batch_norm(
        #     inputs=x['prcp'],
        #     is_training=True,
        #     center=True,
        #     scale=True,
        # )
        # x['daylight'] = tf.contrib.layers.batch_norm(
        #     inputs=x['daylight'],
        #     is_training=True,
        #     center=True,
        #     scale=True,
        # )

        # x["elation"] = tf.contrib.layers.flatten(x["elevation"])
        # x["elevation"] = tf.squeeze(x["elevation"])
        # x["elevation"] = tf.reshape(x["elevation"], [-1])
        # x["elevation"].set_shape((batch_size, 1))
        # x['elevation'] = tf.contrib.layers.flatten(x["elevation"])
        # x["elevation"].set_shape((batch_size, 1))
        # x["tmax"] = tf.squeeze(x["tmax"])
        # x["prcp"] = tf.squeeze(x["prcp"])
        # x["daylight"] = tf.squeeze(x["daylight"])

        # y = tf.squeeze(x.pop("label"))
        y = x.pop("label")
        y = tf.cast(y, tf.bool)
        # y = tf.to_int32(y, name='ToInt32')
        # y.assign(tf.where(tf.equal(y, tf.constant(58583)), tf.zeros_like(y), y))

        # len_key = tf.placeholder(tf.int16, shape=(batch_size))
        # x[rnn_common.RNNKeys.SEQUENCE_LENGTH_KEY] = tf.fill(tf.shape(len_key), FLAGS.days_before_occurrence)

        return x, y

    return _input_fn


def get_rnn_estimator():

    model_dir = tempfile.mkdtemp() if not FLAGS.model_dir else FLAGS.model_dir

    gz = sparse_column_with_hash_bucket(
        column_name="grid-zone",
        hash_bucket_size=1000
    )

    context_feature_columns=[
        # real_valued_column("label", dtype=tf.int64),
        tf.contrib.layers.embedding_column(gz, dimension=8),
        real_valued_column("elevation", dtype=tf.float32)
    ]

    sequence_feature_columns=[
        real_valued_column("tmax", dtype=tf.float32),
        # # real_valued_column("tmin", dtype=tf.float32),
        real_valued_column("prcp", dtype=tf.float32),
        # real_valued_column("daylight", dtype=tf.float32)
    ]

    return dynamic_rnn_estimator.DynamicRnnEstimator(
          problem_type = constants.ProblemType.CLASSIFICATION,
          prediction_type = rnn_common.PredictionType.SINGLE_VALUE,
          sequence_feature_columns = sequence_feature_columns,
          context_feature_columns = context_feature_columns,
          num_units = 100,
          gradient_clipping_norm=1.0,
          predict_probabilities=True,
          num_classes=2,
          cell_type = 'gru',
          optimizer = 'SGD',
          learning_rate = 0.001,
          model_dir=model_dir,
    )

def get_dnn_estimator():

    feature_columns = [
        tf.feature_column.numeric_column("x", shape=[28]),
        tf.contrib.layers.embedding_column(sparse_column_with_hash_bucket(
            column_name="grid",
            hash_bucket_size=1000
        ), dimension=8)
    ]

    return DNNClassifier(
        feature_columns=feature_columns,
        hidden_units=[60, 120, 60],
        n_classes=2,
        optimizer=tf.train.ProximalAdagradOptimizer(
            learning_rate=0.01,
            l1_regularization_strength=0.001
        )
    )


def _create_my_experiment(output_dir):

    # You can change a subset of the run_config properties as
    #   run_config = run_config.replace(save_checkpoints_steps=500)


    return Experiment(
        estimator=get_rnn_estimator(),
        train_input_fn=get_input_fn(FLAGS.train_data, ModeKeys.TRAIN, FLAGS.training_batch_size),
        eval_input_fn=get_input_fn(FLAGS.eval_data, ModeKeys.EVAL, 50),
        eval_metrics={
            'rmse': MetricSpec(
                metric_fn=tf.contrib.metrics.streaming_root_mean_squared_error
            )
        },
    )


def run():

    tf.logging.set_verbosity(tf.logging.INFO)

    estimator = get_dnn_estimator()
    estimator.fit(
        input_fn=get_input_fn(FLAGS.train_data, ModeKeys.TRAIN, FLAGS.training_batch_size),
    )
    response = estimator.evaluate(input_fn=get_input_fn(FLAGS.eval_data, ModeKeys.EVAL, 50))
    print('Accuracy: {0:f}'.format(response["accuracy"]))
    print('GlobalStep: {0:f}'.format(response["global_step"]))
    print('Loss: {0:f}'.format(response["loss"]))
    print(response)

    return

    output_dir = tempfile.mkdtemp()

    try:
        learn_runner.run(_create_my_experiment, output_dir=output_dir)
    except:
        traceback.print_exc()

if __name__ == "__main__":
    run()