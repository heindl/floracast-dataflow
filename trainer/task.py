# Copyright 2017 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#            http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Sample for Reddit dataset can be run as a wide or deep model."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
import json
import os
import sys

import tensorflow as tf

from tensorflow_transform.saved import input_fn_maker
from tensorflow_transform.tf_metadata import metadata_io
from tensorflow.contrib.learn.python.learn import learn_runner

KEY_FEATURE_COLUMN = 'occurrence_id'
TARGET_FEATURE_COLUMN = 'taxon'

DEPLOY_SAVED_MODEL_DIR = 'saved_model'
MODEL_EVALUATIONS_FILE = 'model_evaluations'
BATCH_PREDICTION_RESULTS_FILE = 'batch_prediction_results'


def create_parser():
    """Initialize command line parser using arparse.
    Returns:
      An argparse.ArgumentParser.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--train_data_path', type=str, required=True)
    parser.add_argument('--output_path', type=str, required=True)
    parser.add_argument(
        '--hidden_units',
        nargs='*',
        help='List of hidden units per layer. All layers are fully connected. Ex.'
             '`64 32` means first layer has 64 nodes and second one has 32.',
        default=[512],
        type=int)
    parser.add_argument(
        '--batch_size',
        help='Number of input records used per batch',
        default=30000,
        type=int)
    # parser.add_argument(
    #     '--eval_batch_size',
    #     help='Number of eval records used per batch',
    #     default=5000,
    #     type=int)
    # parser.add_argument(
    #     '--train_steps', help='Number of training steps to perform.', type=int)
    parser.add_argument(
        '--eval_steps',
        help='Number of evaluation steps to perform.',
        type=int,
        default=100)
    parser.add_argument(
        '--train_set_size',
        help='Number of samples on the train dataset.',
        type=int,
        default=60 * 1e6)
    parser.add_argument(
        '--num_epochs', help='Number of epochs', default=5, type=int)
    return parser


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
        tf.contrib.layers.embedding_column(tf.contrib.layers.sparse_column_with_hash_bucket(
            column_name="mgrs_grid_zone",
            hash_bucket_size=1000
        ), dimension=8),
        tf.contrib.layers.real_valued_column("elevation", dtype=tf.float32),
        tf.contrib.layers.real_valued_column("avg_temp", dtype=tf.float32),
        tf.contrib.layers.real_valued_column("max_temp", dtype=tf.float32),
        tf.contrib.layers.real_valued_column("min_temp", dtype=tf.float32),
        tf.contrib.layers.real_valued_column("precipitation", dtype=tf.float32),
        tf.contrib.layers.real_valued_column("daylight", dtype=tf.float32)
    ]


def gzip_reader_fn():
    return tf.TFRecordReader(options=tf.python_io.TFRecordOptions(
        compression_type=tf.python_io.TFRecordCompressionType.GZIP))


def get_transformed_reader_input_fn(transformed_metadata,
                                    transformed_data_paths,
                                    batch_size,
                                    mode):

    """Wrap the get input features function to provide the runtime arguments."""
    return input_fn_maker.build_training_input_fn(
        metadata=transformed_metadata,
        file_pattern=(
            transformed_data_paths[0] if len(transformed_data_paths) == 1
            else transformed_data_paths),
        training_batch_size=batch_size,
        label_keys=[TARGET_FEATURE_COLUMN],
        reader=gzip_reader_fn,
        key_feature_name=KEY_FEATURE_COLUMN,
        reader_num_threads=4,
        queue_capacity=batch_size * 2,
        randomize_input=(mode != tf.contrib.learn.ModeKeys.EVAL),
        num_epochs=(1 if mode == tf.contrib.learn.ModeKeys.EVAL else None))

def get_experiment_fn(args):
    """Wrap the get experiment function to provide the runtime arguments."""

    def get_experiment(output_dir):
        from datetime import datetime
        """Function that creates an experiment http://goo.gl/HcKHlT.
        Args:
          output_dir: The directory where the training output should be written.
        Returns:
          A `tf.contrib.learn.Experiment`.
        """

        runconfig = tf.contrib.learn.RunConfig()
        cluster = runconfig.cluster_spec
        num_table_shards = max(1, runconfig.num_ps_replicas * 3)
        num_partitions = max(1, 1 + cluster.num_tasks('worker') if cluster and
                                                                   'worker' in cluster.jobs else 0)

        model_dir = os.path.join(output_dir, datetime.now().strftime("%s"))

        estimator = tf.contrib.learn.DNNClassifier(
            feature_columns=feature_columns(),
            model_dir=model_dir,
            hidden_units=args.hidden_units,
            n_classes=2,
            optimizer=tf.train.ProximalAdagradOptimizer(
                learning_rate=0.01,
                l1_regularization_strength=0.001
            )
        )

        transformed_metadata = metadata_io.read_metadata(
            os.path.join(args.train_data_path, "transformed_metadata"))

        raw_metadata = metadata_io.read_metadata(
            os.path.join(args.train_data_path, "raw_metadata"))

        serving_input_fn = (
            input_fn_maker.build_parsing_transforming_serving_input_fn(
                raw_metadata=raw_metadata,
                transform_savedmodel_dir=os.path.join(args.train_data_path, "transform_fn"),
                raw_label_keys=[TARGET_FEATURE_COLUMN])
        )
        export_strategy = tf.contrib.learn.utils.make_export_strategy(
            serving_input_fn,
            exports_to_keep=5,
            default_output_alternative_key=None
        )

        train_input_fn = get_transformed_reader_input_fn(
            transformed_metadata,
            os.path.join(args.train_data_path, "features_train-00000-of-00001.tfrecord.gz"),
            args.batch_size,
            tf.contrib.learn.ModeKeys.TRAIN)

        eval_input_fn = get_transformed_reader_input_fn(
            transformed_metadata,
            os.path.join(args.train_data_path, "features_eval-00000-of-00001.tfrecord.gz"),
            args.batch_size,
            tf.contrib.learn.ModeKeys.EVAL)

        return tf.contrib.learn.Experiment(
            estimator=estimator,
            train_steps=(args.num_epochs * args.train_set_size // args.batch_size),
            eval_steps=args.eval_steps,
            train_input_fn=train_input_fn,
            eval_input_fn=eval_input_fn,
            export_strategies=export_strategy,
            min_eval_frequency=500)

    # Return a function to create an Experiment.
    return get_experiment


def main(argv=None):
    """Run a Tensorflow model on the Reddit dataset."""
    env = json.loads(os.environ.get('TF_CONFIG', '{}'))
    # First find out if there's a task value on the environment variable.
    # If there is none or it is empty define a default one.
    task_data = env.get('task') or {'type': 'master', 'index': 0}
    argv = sys.argv if argv is None else argv
    args = create_parser().parse_args(args=argv[1:])

    trial = task_data.get('trial')
    if trial is not None:
        output_dir = os.path.join(args.output_path, trial)
    else:
        output_dir = args.output_path

    learn_runner.run(experiment_fn=get_experiment_fn(args),
                     output_dir=output_dir)


if __name__ == '__main__':
    main()