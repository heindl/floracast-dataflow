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

# from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
import sys
import tensorflow as tf
import train_shared_model, train_shared_input_fn
import functools
import argparse
import shutil

parser = argparse.ArgumentParser()

parser.add_argument(
    '--train_data_path', type=str, required=False, default="/tmp/floracast-datamining/transformed/53713/1513571852")

parser.add_argument(
    '--model_dir', type=str, default='/tmp/morel_model',
    help='Base directory for the model.')


parser.add_argument(
    '--train_epochs', type=int, default=40, help='Number of training epochs.')

parser.add_argument(
    '--epochs_per_eval', type=int, default=2,
    help='The number of training epochs to run between evaluations.')

parser.add_argument(
    '--batch_size', type=int, default=40, help='Number of examples per batch.')

def main(unused_argv):

    # Clean up existing model dir.
    shutil.rmtree(FLAGS.model_dir, ignore_errors=True)

    model = tf.estimator.DNNClassifier(
        feature_columns=train_shared_model.feature_columns(),
        hidden_units=[100, 75, 50, 25],
        model_dir=FLAGS.model_dir
    )

    # Train and evaluate the model every `FLAGS.epochs_per_eval` epochs.
    for n in range(FLAGS.train_epochs // FLAGS.epochs_per_eval):

        model.train(input_fn=functools.partial(train_shared_input_fn.transformed_input_fn,
                                               transformed_location=FLAGS.train_data_path,
                                               batch_size=FLAGS.batch_size,
                                               mode=tf.estimator.ModeKeys.TRAIN,
                                               epochs=FLAGS.epochs_per_eval))

        res = model.evaluate(input_fn=functools.partial(train_shared_input_fn.transformed_input_fn,
                                                        transformed_location=FLAGS.train_data_path,
                                                        batch_size=FLAGS.batch_size,
                                                        mode=tf.estimator.ModeKeys.EVAL,
                                                        epochs=1))


        # Display evaluation metrics
        print('Results at epoch', (n + 1) * FLAGS.epochs_per_eval)
        print('-' * 60)
        for key in sorted(res):
            print('%s: %s' % (key, res[key]))



if __name__ == '__main__':
    tf.logging.set_verbosity(tf.logging.INFO)
    FLAGS, unparsed = parser.parse_known_args()
    tf.app.run(main=main, argv=[sys.argv[0]] + unparsed)