from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import tensorflow as tf
from tensorflow.contrib.learn.python.learn.estimators import constants
from tensorflow.contrib.learn.python.learn.estimators import rnn_common
from tensorflow.contrib.learn.python.learn.estimators import dynamic_rnn_estimator

BATCH_SIZE = 32
SEQUENCE_LENGTH = 16


xc = tf.contrib.layers.real_valued_column("")
estimator = dynamic_rnn_estimator.DynamicRnnEstimator(problem_type = constants.ProblemType.LINEAR_REGRESSION,
                                                 prediction_type = rnn_common.PredictionType.SINGLE_VALUE,
                                                 sequence_feature_columns = [xc],
                                                 context_feature_columns = None,
                                                 num_units = 5,
                                                 cell_type = 'lstm',
                                                 optimizer = 'SGD',
                                                 learning_rate = 0.1)

def get_train_inputs():
    x = tf.random_uniform([BATCH_SIZE, SEQUENCE_LENGTH])
    y = tf.reduce_mean(x, axis=1)
    x = tf.expand_dims(x, axis=2)
    return {"": x}, y

# x, y = get_train_inputs()
# print(tf.contrib.learn.run_n(x, n=1, feed_dict=None))

estimator.fit(input_fn=get_train_inputs, steps=1000)