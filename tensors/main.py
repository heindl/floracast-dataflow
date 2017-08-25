from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import tempfile
import tensorflow as tf
from tensorflow.contrib.layers import real_valued_column, sparse_column_with_hash_bucket
from tensorflow.python.ops import io_ops
from tensorflow.contrib import learn, layers
from tensorflow.contrib.learn.python.learn.estimators import constants
from tensorflow.contrib.learn.python.learn.estimators import rnn_common
from tensorflow.contrib.learn.python.learn.estimators import dynamic_rnn_estimator
# Constants
DAYS_BEFORE_OCCURRENCE = 45
BATCH_SIZE = 10
TRAIN_STEPS=200

flags = tf.app.flags
FLAGS = flags.FLAGS

flags.DEFINE_string("model_dir", "", "Base directory for output models.")
flags.DEFINE_integer("train_steps", TRAIN_STEPS, "Number of training steps.")
flags.DEFINE_string(
    "train_data",
    "",
    "Path to the training data.")
flags.DEFINE_string(
    "test_data",
    "",
    "Path to the test data.")

context_features={
     'elevation': tf.FixedLenFeature(shape=[1], dtype=tf.float32),
     # 'grid-zone': tf.VarLenFeature(dtype=tf.string),
     'label': tf.FixedLenFeature(shape=[1], dtype=tf.int64)
 }

sequence_features={
    "tmax": tf.FixedLenSequenceFeature(shape=[1], dtype=tf.float32),
    # "tmin": tf.FixedLenSequenceFeature(shape=[1], dtype=tf.float32),
    "prcp": tf.FixedLenSequenceFeature(shape=[1], dtype=tf.float32),
    "daylight": tf.FixedLenSequenceFeature(shape=[1], dtype=tf.float32),
}

def print_records(file):

    record_iterator = tf.python_io.tf_record_iterator(file)
    c = 0
    for record in record_iterator:
        c += 1
        example = tf.train.SequenceExample()
        example.ParseFromString(record)
        # print(example.features.feature['label']
        #              .int64_list
        #              .value[0])
        print(example)
        break

    print("total", c)

def input_fn(file, mode, batch_size):

    filename_queue = tf.train.string_input_producer([file], num_epochs=10)

    reader = tf.TFRecordReader()
    _, example = reader.read(filename_queue)

    context_parsed, sequence_parsed = tf.parse_single_sequence_example(
        example,
        context_features=context_features,
        sequence_features=sequence_features
    )

    features = context_parsed.copy()
    features.update(sequence_parsed)
    features['tmax'].set_shape((DAYS_BEFORE_OCCURRENCE,1))
    # features['tmin'].set_shape((DAYS_BEFORE_OCCURRENCE,1))
    features['prcp'].set_shape((DAYS_BEFORE_OCCURRENCE,1))
    features['daylight'].set_shape((DAYS_BEFORE_OCCURRENCE,1))

    x = tf.train.shuffle_batch(
        features,
        batch_size=batch_size,
        capacity=2000,
        min_after_dequeue=20,
        name="read_batch_features_{}".format(mode)
    )

    y = x.pop("label")

    len_key = tf.placeholder(tf.int16, shape=(batch_size))
    x[rnn_common.RNNKeys.SEQUENCE_LENGTH_KEY] = tf.fill(tf.shape(len_key), DAYS_BEFORE_OCCURRENCE)

    return x, y


def build_classifier(file, model_dir):

    gz = sparse_column_with_hash_bucket(
        column_name="grid-zone",
        hash_bucket_size=1000
    )

    context_feature_columns=[
        # real_valued_column("label", dtype=tf.int64),
        # tf.contrib.layers.embedding_column(gz, dimension=8),
        real_valued_column("elevation", dtype=tf.float32, dimension=1)
    ]
    sequence_feature_columns=[
        real_valued_column("tmax", dtype=tf.float32),
        # real_valued_column("tmin", dtype=tf.float32),
        real_valued_column("prcp", dtype=tf.float32),
        real_valued_column("daylight", dtype=tf.float32)
    ]

    return dynamic_rnn_estimator.DynamicRnnEstimator(problem_type = constants.ProblemType.CLASSIFICATION,
                                              prediction_type = rnn_common.PredictionType.MULTIPLE_VALUE,
                                              sequence_feature_columns = sequence_feature_columns,
                                              context_feature_columns = context_feature_columns,
                                              num_units = 20,
                                              predict_probabilities=True,
                                              num_classes=2,
                                              cell_type = 'lstm',
                                              optimizer = 'SGD',
                                              # gradient_clipping_norm=1.0,
                                              learning_rate = 0.01)


def run():
    model_dir = tempfile.mkdtemp() if not FLAGS.model_dir else FLAGS.model_dir
    print("model directory = %s" % model_dir)
    classifier = build_classifier(FLAGS.train_data, model_dir)
    classifier.fit(
        input_fn=lambda: input_fn(FLAGS.train_data, tf.contrib.learn.ModeKeys.TRAIN, 10),
        # steps=FLAGS.train_steps
    )
    results = classifier.evaluate(input_fn=lambda: input_fn(FLAGS.train_data, tf.contrib.learn.ModeKeys.EVAL, 5))
    for key in sorted(results):
        print("%s: %s" % (key, results[key]))

if __name__ == "__main__":
    run()