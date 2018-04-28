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
import tensorflow as tf
import argparse
import sys

from functions import train
# from functions import experiments

from functions import occurrences

parser = argparse.ArgumentParser()

# parser.add_argument(
#     '--name_usage_id', type=str, required=True)

# parser.add_argument(
#     '--transformer', type=str, required=False,
#     help='Use a specific transformer file.')
#
# parser.add_argument(
#     '--model_dir', type=str, required=False,
#     help='Save to a specific directory.')

parser.add_argument(
    '--train_epochs', type=int, default=200, help='Number of training epochs.')
#
parser.add_argument(
    '--epochs_per_eval', type=int, default=4,
    help='The number of training epochs to run between evaluations.')

parser.add_argument(
    '--batch_size', type=int, default=32, help='Number of examples per batch.')

parser.add_argument(
    '--name_usage_id', type=str, default="ugkG3de", help='Number of examples per batch.')

# def main(argv):
#
#     model = tf.estimator.DNNClassifier(
#         feature_columns=[],
#         # hidden_units=[100, 75, 50, 25],
#         hidden_units=[75],
#         model_dir='/tmp/lhYkSxbfSPhG/model')
#     vars = model.get_variable_names()
#     print("Input Length", model.get_variable_value('dnn/logits/bias'))
#     for v in vars:
#         print(v)

    # with tf.Session(graph=tf.Graph()) as sess:
    #     tf.saved_model.loader.load(
    #         sess, [tf.saved_model.tag_constants.SERVING], '/tmp/SQtjlLHyi/model/exports/1524417008')
    #
    #     pb_visual_writer = summary.FileWriter('/tmp/tf_log_dir')
    #     pb_visual_writer.add_graph(sess.graph)
    #     print("Model Imported. Visualize by running: "
    #           "tensorboard --logdir={}".format('/tmp/tf_log_dir'))

def main(argv):

    # exp = experiments.Experiments()

    occurrence_records = occurrences.OccurrenceTFRecords(
        name_usage_id=FLAGS.name_usage_id,
        project="floracast-firestore",
        gcs_bucket="floracast-datamining",
        # occurrence_path="/tmp/occurrence-data-keep/occurrences/",
        random_path="/tmp/occurrence-data-keep/random/",
        multiplier_of_random_to_occurrences=1,
        test_train_split_percentage=0.1,
    )

    # print("Total Experiments", exp.count())

    # for experiment_number in range(exp.count()):

    training_data = train.TrainingData(
        project="floracast-firestore",
        gcs_bucket="floracast-datamining",
        name_usage_id=FLAGS.name_usage_id,
        train_batch_size=FLAGS.batch_size,
        train_epochs=FLAGS.epochs_per_eval,
        occurrence_records=occurrence_records,
        transform_data_path='/tmp/transform-data-keep',
        # experiment=exp.get(experiment_number)
        # model_path='/tmp/SQtjlLHyi/model'
    )

    model = training_data.get_estimator()

    # Train and evaluate the model every `FLAGS.epochs_per_eval` epochs.
    for n in range(FLAGS.train_epochs // FLAGS.epochs_per_eval):

        eval_input_fn, train_input_fn = training_data.input_functions()

        model.train(input_fn=train_input_fn)

        res = model.evaluate(input_fn=eval_input_fn)
        #
        # for v in model.get_variable_names():
        #     print(v)
        # Hidden Layer 0 bias length would be 100 if [100, 50, 25] were given weights.
        # dnn/hiddenlayer_0/bias
        # dnn/hiddenlayer_0/bias/t_0/Adagrad
        # dnn/hiddenlayer_0/kernel
        # dnn/hiddenlayer_0/kernel/t_0/Adagrad
        # dnn/hiddenlayer_1/bias
        # dnn/hiddenlayer_1/bias/t_0/Adagrad
        # dnn/hiddenlayer_1/kernel
        # dnn/hiddenlayer_1/kernel/t_0/Adagrad
        # dnn/hiddenlayer_2/bias
        # dnn/hiddenlayer_2/bias/t_0/Adagrad
        # dnn/hiddenlayer_2/kernel
        # dnn/hiddenlayer_2/kernel/t_0/Adagrad
        # dnn/hiddenlayer_3/bias
        # dnn/hiddenlayer_3/bias/t_0/Adagrad
        # dnn/hiddenlayer_3/kernel
        # dnn/hiddenlayer_3/kernel/t_0/Adagrad
        # dnn/input_from_feature_columns/input_layer/s2_token_3_embedding/embedding_weights
        # dnn/input_from_feature_columns/input_layer/s2_token_3_embedding/embedding_weights/t_0/Adagrad
        # dnn/input_from_feature_columns/input_layer/s2_token_4_embedding/embedding_weights
        # dnn/input_from_feature_columns/input_layer/s2_token_4_embedding/embedding_weights/t_0/Adagrad
        # dnn/logits/bias
        # dnn/logits/bias/t_0/Adagrad
        # dnn/logits/kernel
        # dnn/logits/kernel/t_0/Adagrad

        # print(model.get_variable_value('dnn/input_from_feature_columns/input_layer/s2_token_3_embedding/embedding_weights'))
        print(res)

        # exp.register_eval(experiment_number, res)

    training_data.export_model()

    # training_data.upload_exported_model()

    # exp.print_tsv()


if __name__ == '__main__':
    # tf.logging.set_verbosity(tf.logging.INFO)
    tf.logging.set_verbosity(tf.logging.ERROR)
    FLAGS, unparsed = parser.parse_known_args()
    tf.app.run(main=main, argv=[sys.argv[0]] + unparsed)