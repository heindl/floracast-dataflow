from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import input as input_data

import tempfile
# import urllib
# import pandas as pd
import tensorflow as tf

flags = tf.app.flags
FLAGS = flags.FLAGS

flags.DEFINE_string("model_dir", "", "Base directory for output models.")
flags.DEFINE_string("model_type", "wide_n_deep",
                    "Valid model types: {'wide', 'deep', 'wide_n_deep'}.")
flags.DEFINE_integer("train_steps", 200, "Number of training steps.")
flags.DEFINE_string(
    "train_data",
    "",
    "Path to the training data.")
flags.DEFINE_string(
    "test_data",
    "",
    "Path to the test data.")


# def maybe_download():
#   """May be downloads training data and returns train and test file names."""
#   if FLAGS.train_data:
#     train_file_name = FLAGS.train_data
#   else:
#     train_file = tempfile.NamedTemporaryFile(delete=False)
#     urllib.urlretrieve("https://archive.ics.uci.edu/ml/machine-learning-databases/adult/adult.data", train_file.name)  # pylint: disable=line-too-long
#     train_file_name = train_file.name
#     train_file.close()
#     print("Training data is downloaded to %s" % train_file_name)
#
#   if FLAGS.test_data:
#     test_file_name = FLAGS.test_data
#   else:
#     test_file = tempfile.NamedTemporaryFile(delete=False)
#     urllib.urlretrieve("https://archive.ics.uci.edu/ml/machine-learning-databases/adult/adult.test", test_file.name)  # pylint: disable=line-too-long
#     test_file_name = test_file.name
#     test_file.close()
#     print("Test data is downloaded to %s" % test_file_name)
#
#   return train_file_name, test_file_name


def build_estimator(model_dir):

    context_columns = [
        tf.contrib.layers.sparse_column_with_hash_bucket(
            column_name="grid-zone",
            hash_bucket_size=1000
        ),
        tf.contrib.layers.real_valued_column("elevation", dtype=tf.float32, dimension=1)
    ]

    # Sequence Columns
    sequence_columns = [
        tf.contrib.layers.real_valued_column("tmax", dtype=tf.float32, dimension=60),
        tf.contrib.layers.real_valued_column("tmin", dtype=tf.float32, dimension=60),
        tf.contrib.layers.real_valued_column("prcp", dtype=tf.float32, dimension=60),
        tf.contrib.layers.real_valued_column("daylight", dtype=tf.float32, dimension=60),
    ]

    # Wide columns and deep columns.
    # wide_columns = [gender, native_country, education, occupation, workclass,
    #                 marital_status, relationship, age_buckets,
    #                 tf.contrib.layers.crossed_column([education, occupation],
    #                                                  hash_bucket_size=int(1e4)),
    #                 tf.contrib.layers.crossed_column(
    #                     [age_buckets, race, occupation],
    #                     hash_bucket_size=int(1e6)),
    #                 tf.contrib.layers.crossed_column([native_country, occupation],
    #                                                  hash_bucket_size=int(1e4))]
    # deep_columns = [
    #     tf.contrib.layers.embedding_column(workclass, dimension=8),
    #     tf.contrib.layers.embedding_column(education, dimension=8),
    #     tf.contrib.layers.embedding_column(marital_status,
    #                                        dimension=8),
    #     tf.contrib.layers.embedding_column(gender, dimension=8),
    #     tf.contrib.layers.embedding_column(relationship, dimension=8),
    #     tf.contrib.layers.embedding_column(race, dimension=8),
    #     tf.contrib.layers.embedding_column(native_country,
    #                                        dimension=8),
    #     tf.contrib.layers.embedding_column(occupation, dimension=8),
    #     age,
    #     education_num,
    #     capital_gain,
    #     capital_loss,
    #     hours_per_week,
    # ]

    return tf.contrib.learn.DynamicRnnEstimator(
        problem_type = tf.contrib.learn.ProblemType.CLASSIFICATION,
        prediction_type = tf.contrib.learn.PredictionType.MULTIPLE_VALUE,
        sequence_feature_columns = sequence_columns,
        context_feature_columns = context_columns,
        num_classes=2,
        cell_type='gru',
        predict_probabilities=True,
        model_dir=model_dir,
    )


def train_and_eval():
    model_dir = tempfile.mkdtemp() if not FLAGS.model_dir else FLAGS.model_dir
    print("model directory = %s" % model_dir)
    m = build_estimator(model_dir)
    m.fit(input_fn=lambda: input_data.input_fn(tf.contrib.learn.ModeKeys.TRAIN, FLAGS.train_data, 128), steps=FLAGS.train_steps)
    results = m.evaluate(input_fn=lambda: input_data.input_fn(tf.contrib.learn.ModeKeys.EVAL, FLAGS.test_data, 128), steps=1)
    for key in sorted(results):
        print("%s: %s" % (key, results[key]))


def main(_):
    train_and_eval()


if __name__ == "__main__":
    tf.app.run()