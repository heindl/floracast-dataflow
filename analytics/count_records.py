import tensorflow as tf
# from tensorflow_transform.tf_metadata import metadata_io
# import base64, io, json  # pylint: disable=g-import-not-at-top
from tensorflow.python.lib.io import tf_record
TFRecordCompressionType = tf_record.TFRecordCompressionType
from glob import iglob

options = tf_record.TFRecordOptions(TFRecordCompressionType.GZIP)
total = 0
taxa = {}
# with io.open('./data.txt', 'w', encoding='utf-8') as f:
for filename in iglob('/Users/m/Desktop/phenograph/infra/src/bitbucket.org/heindl/dataflow/gs/floracast-models/forests/1510156600/*.gz'):
    print(filename)
    for example in tf.python_io.tf_record_iterator(filename, options=options):
        # e = tf.train.Example.FromString(example)
        total = total + 1
        # taxon = e.features.feature["taxon"].bytes_list.value[0]
        # if taxon in taxa:
        #     taxa[taxon] += 1
        # else:
        #     taxa[taxon] = 1

print("total tf records: ", total)