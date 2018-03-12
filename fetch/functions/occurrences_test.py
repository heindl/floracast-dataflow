import unittest
from occurrences import OccurrenceTFRecords


class TFRecordsTestCase(unittest.TestCase):

    def test_tf_records_count(self):
        """Do we recieve the correct tfrecords count?"""

        parser = OccurrenceTFRecords(
            name_usage_id="9sykdre6ougztwabsjjufiwvu",
            project="floracast-firestore",
            gcs_bucket="floracast-datamining",
        )
        self.assertEqual(parser._total_count, 555)
        self.assertEqual(parser._occurrence_count, 192)
        self.assertEqual(parser._eval_count(0.05), 28)
        self.assertEqual(len(parser._generate_random(0.05)), 28)

        eval_file, train_file = parser.train_test_split(0.05)

        initial_eval_total = parser.count(eval_file)
        self.assertEqual(initial_eval_total, 28)

        initial_train_total = parser.count(train_file)
        self.assertEqual(initial_train_total, 527)

        parser.train_test_split(0.05)
        secondary_eval_total = parser.count(eval_file)
        self.assertEqual(secondary_eval_total, 28)

        secondary_train_total = parser.count(train_file)
        self.assertEqual(secondary_train_total, 527)




if __name__ == '__main__':
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(TFRecordsTestCase)
    unittest.TextTestRunner().run(suite)