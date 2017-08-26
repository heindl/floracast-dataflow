import tensorflow as tf
from tensorflow.python.framework import ops
from tensorflow.python.ops import variables
from tensorflow.python.training import coordinator
from tensorflow.python.training import queue_runner_impl
from tensorflow.python.framework import errors
from main import input_fn, print_records
from tensorflow.contrib.layers import real_valued_column

DAYS_BEFORE_OCCURRENCE = 45
BATCH_SIZE=2

# python -m unittest -v main_test.InputFnTest.testSquare

class InputFnTest(tf.test.TestCase):

    def testSquare(self):

        file = "/Users/m/Desktop/phenograph/infra/src/bitbucket.org/heindl/dataflow/tensors/occurrences.tfrecord-00000-of-00001.tfrecord"
        # c = 0
        # for record in tf.python_io.tf_record_iterator(file):
        #     if c == 0:
        #         print(record)
        #     c += 1
        # print_records(file)
        # return
        # x, y = input_fn(1, file, 10)
        # print(x)
        # print(x)

        x, y = input_fn(file, tf.contrib.learn.ModeKeys.TRAIN, 3)
        sequence = tf.contrib.learn.run_n({'y': y}, n=1, feed_dict=None)
        print sequence
        # input_features = set([
        #     tf.contrib.layers.real_valued_column("label", dtype=tf.int64),
        #     tf.contrib.layers.sparse_column_with_hash_bucket(
        #         column_name="grid-zone",
        #         hash_bucket_size=1000
        #     ),
        #     tf.contrib.layers.real_valued_column("elevation", dtype=tf.float32, dimension=1),
        #     tf.contrib.layers.real_valued_column("tmax", dtype=tf.float32, dimension=DAYS_BEFORE_OCCURRENCE),
        #     tf.contrib.layers.real_valued_column("tmin", dtype=tf.float32, dimension=DAYS_BEFORE_OCCURRENCE),
        #     tf.contrib.layers.real_valued_column("prcp", dtype=tf.float32, dimension=DAYS_BEFORE_OCCURRENCE),
        #     tf.contrib.layers.real_valued_column("daylight", dtype=tf.float32, dimension=DAYS_BEFORE_OCCURRENCE),
        # ])


        # context_feature_columns=[
        #     real_valued_column("label", dtype=tf.int64),
        #     # tf.contrib.layers.embedding_column(gz, dimension=8),
        #     real_valued_column("elevation", dtype=tf.float32, dimension=1)
        # ]
        # sequence_feature_columns=[
        #     real_valued_column("tmax", dtype=tf.float32, dimension=1),
        #     real_valued_column("tmin", dtype=tf.float32, dimension=1),
        #     real_valued_column("prcp", dtype=tf.float32, dimension=1),
        #     real_valued_column("daylight", dtype=tf.float32, dimension=1)
        # ]
        # cxt, seq = feature_columns(file)
        # print(cxt['elevation'])

        # with ops.Graph().as_default() as g, self.test_session(graph=g) as sess:
        #
        #     features = tf.contrib.layers.create_feature_spec_for_parsing(input_features)
        #
        #     print(features)
        #
        #     feature_map = tf.contrib.learn.io.read_batch_record_features(
        #         file_pattern=[file],
        #         batch_size=BATCH_SIZE,
        #         features=features,
        #         name="read_batch_features_{}".format(tf.contrib.learn.ModeKeys.TRAIN),
        #         randomize_input=True,
        #         num_epochs=1,
        #     )
        #
        #     target = feature_map.pop("label")
        #
        #     sess.run(tf.initialize_all_variables())
        #     coord = coordinator.Coordinator()
        #     threads = queue_runner_impl.start_queue_runners(sess, coord=coord)
        #
        #
        #     print(sess.run(feature_map))
        #     # with self.assertRaises(errors.OutOfRangeError):
        #     #     sess.run(feature_map)
        #
        #     coord.request_stop()
        #     coord.join(threads)
        #     sess.close()



if __name__ == '__main__':
    tf.test.main()