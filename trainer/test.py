import task
import tensorflow as tf
from tensorflow_transform.tf_metadata import metadata_io


raw_metadata = metadata_io.read_metadata(
    "/Users/m/Desktop/phenograph/infra/src/bitbucket.org/heindl/dataflow/gs/floracast-models/train/1505519173/transformed_metadata/")
input_fn = task.get_transformed_reader_input_fn(
           raw_metadata,
           "/Users/m/Desktop/phenograph/infra/src/bitbucket.org/heindl/dataflow/gs/floracast-models/train/1505519173/train_data/*.gz",
           1000,
            tf.estimator.ModeKeys.TRAIN)

x, y = input_fn()

y = tf.contrib.learn.run_n({'y': y})
print(y)