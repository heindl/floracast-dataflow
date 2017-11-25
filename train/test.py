import task
import tensorflow as tf
from tensorflow_transform.tf_metadata import metadata_io
import base64, io, json  # pylint: disable=g-import-not-at-top
from tensorflow.python.lib.io import tf_record
TFRecordCompressionType = tf_record.TFRecordCompressionType
from glob import iglob
from datetime import datetime


# raw_metadata = metadata_io.read_metadata(
#     "/Users/m/Desktop/phenograph/infra/src/bitbucket.org/heindl/dataflow/gs/floracast-models/train/1505519173/transformed_metadata/")
# input_fn = task.get_transformed_reader_input_fn(
#            raw_metadata,
#            "/Users/m/Desktop/phenograph/infra/src/bitbucket.org/heindl/dataflow/gs/floracast-models/train/1505519173/train_data/*.gz",
#            1000,
#             tf.estimator.ModeKeys.TRAIN)
#

# input_fn=task.get_test_prediction_data_fn(args={
#     "train_data_path": "/Users/m/Desktop/phenograph/infra/src/bitbucket.org/heindl/dataflow/gs/floracast-models/train/1505856591"
# },
#     raw_data_file_pattern="/Users/m/Downloads/forests-data.tfrecords"
# )
# x, y = input_fn()
#
# y = tf.contrib.learn.run_n({'x': x})
# print(y)

# serving_input_func = task.get_serving_input_fn(args={
#             "train_data_path": "/Users/m/Desktop/phenograph/infra/src/bitbucket.org/heindl/dataflow/gs/floracast-models/train/1505856591"
#         },
#         raw_label_keys=['taxon'])
#
# print(serving_input_func())

options = tf_record.TFRecordOptions(TFRecordCompressionType.GZIP)
# writer = tf.python_io.TFRecordWriter("/Users/m/Desktop/phenograph/infra/src/bitbucket.org/heindl/dataflow/gs/floracast-models/occurrences/1505437167/2.tfrecord.gz", options=options)

total = 0
# with io.open('./data.txt', 'w', encoding='utf-8') as f:
for filename in iglob('/Users/m/Desktop/phenograph/infra/src/bitbucket.org/heindl/dataflow/gs/floracast-models/forests/1508371092/*.gz'):
    # print(filename)
    for example in tf.python_io.tf_record_iterator(filename, options=options):
        e = tf.train.Example.FromString(example)

        print(e.features.feature["latitude"].float_list.value[0],
              e.features.feature["longitude"].float_list.value[0],
              datetime.fromtimestamp(e.features.feature["date"].int64_list.value[0]).strftime("%Y%m%d"))

        total = total + 1
    print("total", total)
    # _ = e.features.feature.pop("taxon")
    # e.features.feature["taxon"].bytes_list.value.append(val)
    # writer.write(e.SerializeToString())

# writer.close()
        # f.write(unicode(json.dumps({'b64': base64.b64encode(example)}, ensure_ascii=False)))