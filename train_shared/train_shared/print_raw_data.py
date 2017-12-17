import tensorflow as tf
from tensorflow.python.lib.io import tf_record
TFRecordCompressionType = tf_record.TFRecordCompressionType
from glob import iglob

options = tf_record.TFRecordOptions(TFRecordCompressionType.GZIP)
random = 0
total = 0
# /eval_data/*.gz
for filename in iglob("/tmp/floracast-datamining/transformed/58682/1511615426" + '/train_data/*.gz'):
    for example in tf.python_io.tf_record_iterator(filename, options=options):
        e = tf.train.Example.FromString(example)
        txn = e.features.feature["taxon"].bytes_list.value[0]
        print(txn, e.features.feature["daylight"].float_list)
        if txn == '0':
            random = random + 1
        # print(e.features.feature["latitude"].float_list.value[0],
        #       e.features.feature["longitude"].float_list.value[0],
        #       datetime.fromtimestamp(e.features.feature["date"].int64_list.value[0]).strftime("%Y%m%d"))

        total = total + 1
print("total", total)
print("random", random)