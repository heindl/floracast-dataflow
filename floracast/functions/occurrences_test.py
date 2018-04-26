import unittest
from occurrences import OccurrenceTFRecords

from tensorflow.python.lib.io import tf_record
TFRecordCompressionType = tf_record.TFRecordCompressionType


class TFRecordsTestCase(unittest.TestCase):

    def test_tf_records_count_five(self):
        """Do we recieve the correct tfrecords count?"""

        parser = OccurrenceTFRecords(
            name_usage_id="qWlT2bh",
            project="floracast-firestore",
            gcs_bucket="floracast-datamining",
            occurrence_path="/tmp/dHB79w2po/occurrences/",
            random_path="/tmp/dHB79w2po/random/",
        )

        self.assertEqual(parser._eval_random_count, 51)
        self.assertEqual(parser._eval_occurrence_count, 10)

        self.assertEqual(parser._train_random_count, 959)
        self.assertEqual(parser._train_occurrence_count, 192)

        eval_file, train_file = parser.train_test_split()

        initial_eval_total = OccurrenceTFRecords.count(eval_file, compression_type=TFRecordCompressionType.GZIP)
        self.assertEqual(initial_eval_total, 61)

        print(OccurrenceTFRecords.get_first_id(eval_file, compression_type=TFRecordCompressionType.GZIP))

        initial_eval_total = OccurrenceTFRecords.count(train_file, compression_type=TFRecordCompressionType.GZIP)
        self.assertEqual(initial_eval_total, 1151)

        print(OccurrenceTFRecords.get_first_id(train_file, compression_type=TFRecordCompressionType.GZIP))



        eval_file, train_file = parser.train_test_split()

        initial_eval_total = OccurrenceTFRecords.count(eval_file, compression_type=TFRecordCompressionType.GZIP)
        self.assertEqual(initial_eval_total, 61)

        print(OccurrenceTFRecords.get_first_id(eval_file, compression_type=TFRecordCompressionType.GZIP))

        initial_eval_total = OccurrenceTFRecords.count(train_file, compression_type=TFRecordCompressionType.GZIP)
        self.assertEqual(initial_eval_total, 1151)

        print(OccurrenceTFRecords.get_first_id(train_file, compression_type=TFRecordCompressionType.GZIP))

    def test_tf_records_count_one(self):
        """Do we recieve the correct tfrecords count?"""

        parser = OccurrenceTFRecords(
            name_usage_id="qWlT2bh",
            project="floracast-firestore",
            gcs_bucket="floracast-datamining",
            occurrence_path="/tmp/dHB79w2po/occurrences/",
            random_path="/tmp/dHB79w2po/random/",
            multiplier_of_random_to_occurrences=1,
            test_train_split_percentage=0.1,
        )

        self.assertEqual(parser._eval_random_count, 20)
        self.assertEqual(parser._eval_occurrence_count, 20)

        self.assertEqual(parser._train_random_count, 182)
        self.assertEqual(parser._train_occurrence_count, 182)

        eval_file, train_file = parser.train_test_split()

        initial_eval_total = OccurrenceTFRecords.count(eval_file, compression_type=TFRecordCompressionType.GZIP)
        self.assertEqual(initial_eval_total, 40)

        print(OccurrenceTFRecords.get_first_id(eval_file, compression_type=TFRecordCompressionType.GZIP))

        initial_eval_total = OccurrenceTFRecords.count(train_file, compression_type=TFRecordCompressionType.GZIP)
        self.assertEqual(initial_eval_total, 364)

        print(OccurrenceTFRecords.get_first_id(train_file, compression_type=TFRecordCompressionType.GZIP))



if __name__ == '__main__':
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(TFRecordsTestCase)
    unittest.TextTestRunner().run(suite)