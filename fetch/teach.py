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
import argparse
from functions import transform

parser = argparse.ArgumentParser()

parser.add_argument(
    '--train_data_path', type=str, required=True)

parser.add_argument(
    '--model_dir', type=str, required=True,
    help='Base directory for the model.')

parser.add_argument(
    '--train_epochs', type=int, default=20, help='Number of training epochs.')

parser.add_argument(
    '--epochs_per_eval', type=int, default=2,
    help='The number of training epochs to run between evaluations.')

parser.add_argument(
    '--batch_size', type=int, default=20, help='Number of examples per batch.')

def main(argv):

    training_data = transform.TrainingData(
        train_data_path=FLAGS.train_data_path,
        model_path=FLAGS.model_dir,
        train_batch_size=FLAGS.batch_size,
        train_epochs=FLAGS.epochs_per_eval,
    )

    model = training_data.get_estimator()

    # Train and evaluate the model every `FLAGS.epochs_per_eval` epochs.
    for n in range(FLAGS.train_epochs // FLAGS.epochs_per_eval):

        eval_input_fn, train_input_fn = training_data.input_functions(0.05)

        model.train(input_fn=train_input_fn)

        res = model.evaluate(input_fn=eval_input_fn)

        # Display evaluation metrics
        print('Results at epoch', (n + 1) * FLAGS.epochs_per_eval)
        print('-' * 60)
        for key in sorted(res):
            print('%s: %s' % (key, res[key]))


if __name__ == '__main__':
    tf.logging.set_verbosity(tf.logging.INFO)
    FLAGS, unparsed = parser.parse_known_args()
    tf.app.run(main=main, argv=[sys.argv[0]] + unparsed)