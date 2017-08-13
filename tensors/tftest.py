#  Copyright 2016 The TensorFlow Authors. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
"""Example of recurrent neural networks over characters for DBpedia dataset.
This model is similar to one described in this paper:
   "Character-level Convolutional Networks for Text Classification"
   http://arxiv.org/abs/1509.01626
and is somewhat alternative to the Lua code from here:
   https://github.com/zhangxiangxiao/Crepe
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
import argparse
import tensorflow as tf

context_features = {
    'label': tf.FixedLenFeature([], dtype=tf.int64),
    # 'date': tf.FixedLenFeature([], dtype=tf.int64),
    # 'latitude': tf.FixedLenFeature([], dtype=tf.float32),
    # 'longitude': tf.FixedLenFeature([], dtype=tf.float32),
    # Military Grid Reference System (MGRS)
    'grid-zone': tf.FixedLenFeature([], dtype=tf.string),
    'elevation': tf.FixedLenFeature([], dtype=tf.float32)
}

sequence_features = {
    'tmax': tf.FixedLenSequenceFeature([60], dtype=tf.float32),
    'tmin': tf.FixedLenSequenceFeature([60], dtype=tf.float32),
    'prcp': tf.FixedLenSequenceFeature([60], dtype=tf.float32),
    'daylight': tf.FixedLenSequenceFeature([60], dtype=tf.float32),
}

def context_features():
    labels = tf.contrib.layers.sparse_column_with_hash_bucket(column_name="label", hash_bucket_size=1000)
    grid_zone = tf.contrib.layers.sparse_column_with_hash_bucket(column_name="grid-zone", hash_bucket_size=1000)

    return

if __name__ == '__main__':

    filename_queue = tf.train.string_input_producer(["./output.csv"], num_epochs=None)

    reader = tf.TFRecordReader()

    _, serialized_example = reader.read(filename_queue)

    context, sequence = tf.parse_single_sequence_example(
        serialized = serialized_example,
        context_features = context_features,
        sequence_features = sequence_features
    )



    feature_columns = tf.contrib.layers.parse_feature_columns_from_sequence_examples(
        serialized_example,
        context_feature_columns=context_features,
        sequence_feature_columns=sequence_features
    )

    print(context['label'])