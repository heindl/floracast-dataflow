import tensorflow as tf
from random import randint, choice
from math import ceil, floor
from example import Example
from string import ascii_letters, digits
from tensorflow.python.lib.io import tf_record
TFRecordCompressionType = tf_record.TFRecordCompressionType
from glob import iglob
import os
import errno

TEMP_DIRECTORY = "/tmp/floracast-model-training/"

class TFRecordParser:

    _total_count = 0
    _occurrence_count = 0

    def __init__(self, tfrecords_path):
        self._filepath = tfrecords_path
        self._total_count, self._occurrence_count = self.count(tfrecords_path)
        self._output_path = TEMP_DIRECTORY + "".join(choice(ascii_letters + digits) for x in range(randint(8, 12)))
        self._eval_output = self._output_path + "/eval.tfrecords.gz"
        self._train_output = self._output_path + "/train.tfrecords.gz"
        if self._total_count == 0:
            raise ValueError("No TFRecords counted")

    @staticmethod
    def is_occurrence(s):
        e = Example()
        e.decode_from_string(s)
        return e.category().lower() != "random"

    @staticmethod
    def count(filepath):
        options = tf_record.TFRecordOptions(TFRecordCompressionType.GZIP)
        total = 0
        occurrences = 0
        for _name in iglob(filepath):
            for e in tf.python_io.tf_record_iterator(_name, options=options):
                total += 1
                if TFRecordParser.is_occurrence(e):
                    occurrences += 1
        return total, occurrences

    def _eval_count(self, percentage_split):
        return int(ceil(self._total_count * percentage_split))

    def _generate_random(self, percentage_split):
        random_points = set()

        while len(random_points) < self._eval_count(percentage_split):
            random_points.add(randint(0, self._total_count-1))
        return random_points

    def _prepare_eval_train_files(self):

        if not self._output_path.startswith(TEMP_DIRECTORY):
            raise ValueError("Invalid TFRecords Output Path")
        try:
            os.makedirs(self._output_path)
        except OSError as exc:
            if exc.errno != errno.EEXIST:
                raise
            pass

        if not self._eval_output.startswith(TEMP_DIRECTORY) or not self._eval_output.startswith(TEMP_DIRECTORY):
            raise ValueError("Invalid Eval/Train Output")

        open(self._eval_output, "w").close()
        open(self._train_output, "w").close()

    def train_test_split(self, percentage_split):

        if percentage_split >= 1:
            raise ValueError("Percentage split is expected to be less than 1")

        if not self._eval_output.startswith(TEMP_DIRECTORY) or not self._eval_output.startswith(TEMP_DIRECTORY):
            raise ValueError("Invalid Eval/Train Output")

        random_points = self._generate_random(percentage_split)

        self._prepare_eval_train_files()

        options = tf_record.TFRecordOptions(TFRecordCompressionType.GZIP)
        eval_writer = tf.python_io.TFRecordWriter(self._eval_output, options=options)
        train_writer = tf.python_io.TFRecordWriter(self._train_output, options=options)

        i = 0
        for _name in iglob(self._filepath):
            for e in tf.python_io.tf_record_iterator(_name, options=options):
                if i in random_points:
                    eval_writer.write(e)
                else:
                    train_writer.write(e)
                i += 1

        eval_writer.close()
        train_writer.close()

        return self._eval_output, self._train_output

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

# options = tf_record.TFRecordOptions(TFRecordCompressionType.GZIP)
# writer = tf.python_io.TFRecordWriter("/Users/m/Desktop/phenograph/infra/src/bitbucket.org/heindl/dataflow/gs/floracast-models/occurrences/1505437167/2.tfrecord.gz", options=options)

# total = 0
# taxa = {}
# # with io.open('./data.txt', 'w', encoding='utf-8') as f:
# for filename in iglob('/Users/m/Desktop/phenograph/infra/src/bitbucket.org/heindl/dataflow/gs/floracast-models/occurrences/1508609812/*.gz'):
#     # print(filename)
#     for example in tf.python_io.tf_record_iterator(filename, options=options):
#         e = tf.train.Example.FromString(example)
#         total = total + 1
#         taxon = e.features.feature["taxon"].bytes_list.value[0]
#         if taxon in taxa:
#             taxa[taxon] += 1
#         else:
#             taxa[taxon] = 1
#
# print("total occurrences: ", total)
# print("total taxa: ", len(taxa.keys()))
# print("occurrences per taxa: ")
# for k, v in sorted(taxa.items(), key=operator.itemgetter(1)):
#     print(k, v)
    # _ = e.features.feature.pop("taxon")
    # e.features.feature["taxon"].bytes_list.value.append(val)
    # writer.write(e.SerializeToString())

    # writer.close()
    # f.write(unicode(json.dumps({'b64': base64.b64encode(example)}, ensure_ascii=False)))