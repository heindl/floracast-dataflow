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
        self.assertEqual(parser._total_count, 434)
        self.assertEqual(parser._occurrence_count, 192)
        self.assertEqual(parser._eval_count(0.05), 22)
        self.assertEqual(len(parser._generate_random(0.05)), 22)

        eval_file, train_file = parser.train_test_split(0.05)

        initial_eval_total, initial_eval_occurrences = parser.count(eval_file)
        self.assertEqual(initial_eval_total, 22)
        # print("intitial_eval_occurrences", initial_eval_occurrences)
        self.assertGreater(initial_eval_occurrences, 1)

        initial_train_total, initial_train_occurrences = parser.count(train_file)
        self.assertEqual(initial_train_total, 412)
        self.assertGreater(initial_train_occurrences, 50)
        # print("initial_train_occurrences", initial_train_occurrences)

        parser.train_test_split(0.05)
        secondary_eval_total, secondary_eval_occurrences = parser.count(eval_file)
        self.assertEqual(secondary_eval_total, 22)
        self.assertNotEqual(initial_eval_occurrences, secondary_eval_occurrences)
        self.assertGreater(secondary_eval_occurrences, 1)
        # print("intitial_eval_occurrences", secondary_eval_occurrences)

        secondary_train_total, secondary_train_occurrences = parser.count(train_file)
        self.assertEqual(secondary_train_total, 412)
        self.assertNotEqual(secondary_train_occurrences, initial_train_occurrences)
        # print("secondary_train_occurrences", secondary_train_occurrences)




if __name__ == '__main__':
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(TFRecordsTestCase)
    unittest.TextTestRunner().run(suite)