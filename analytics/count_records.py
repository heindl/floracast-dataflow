import tensorflow as tf
# from tensorflow_transform.tf_metadata import metadata_io
# import base64, io, json  # pylint: disable=g-import-not-at-top
from tensorflow.python.lib.io import tf_record
TFRecordCompressionType = tf_record.TFRecordCompressionType
from glob import iglob
import argparse

parser = argparse.ArgumentParser(description='Process directory to count')
parser.add_argument('--path', type=str, help='path to tfrecords')
args = parser.parse_args()

options = tf_record.TFRecordOptions(TFRecordCompressionType.GZIP)
total = 0
taxa = {}

# from google.cloud import storage, exceptions
# client = storage.client.Client(project=project)
# try:
#     bucket = client.get_bucket('floracast-configuration')
# except exceptions.NotFound:
#     print('Sorry, that bucket does not exist!')
#     return

# with io.open('./data.txt', 'w', encoding='utf-8') as f:
for filename in iglob(args.path + "/*.tfrecord.gz"):
    print(filename)
    insufficient = 0
    for example in tf.python_io.tf_record_iterator(filename, options=options):
        e = tf.train.Example.FromString(example)
        total = total + 1
        if len(e.features.feature["daylight"].float_list.value) < 90:
            insufficient = insufficient + 1
        # floater = 0
        # for k in e.features.feature["daylight"].float_list[0]:
        #     floater = floater + 1
        # print("float_length", floater)
        # if taxon in taxa:
        #     taxa[taxon] += 1
        # else:
        #     taxa[taxon] = 1
    print("insufficient", insufficient)
print("total tf records: ", total)