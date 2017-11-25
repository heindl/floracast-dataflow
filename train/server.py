from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np
import tensorflow as tf
from grpc.beta import implementations
from tensorflow_serving.apis import predict_pb2
from tensorflow_serving.apis import prediction_service_pb2

tf.app.flags.DEFINE_string('server', 'localhost:9000',
                           'Server host:port.')
tf.app.flags.DEFINE_string('model', 'default',
                           'Model name.')
FLAGS = tf.app.flags.FLAGS

def main(_):

    host, port = FLAGS.server.split(':')
    channel = implementations.insecure_channel(host, int(port))
    stub = prediction_service_pb2.beta_create_PredictionService_stub(channel)

    request = predict_pb2.PredictRequest()
    request.model_spec.name = FLAGS.model
    request.model_spec.signature_name = 'serving_default'

    for serialized in tf.python_io.tf_record_iterator("/Users/m/Downloads/forests-data.tfrecords"):

        request.inputs['inputs'].CopyFrom(
            tf.contrib.util.make_tensor_proto(serialized, shape=[1]))

        result_future = stub.Predict.future(request, 5.0)
        print(result_future.result().outputs)



if __name__ == '__main__':
    tf.app.run()