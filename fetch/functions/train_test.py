import unittest
from train import TrainingData


class TrainingDataTestCase(unittest.TestCase):

    _PROJECT = "floracast-firestore"

    def test_training_data(self):
        """Are random points correctly fetched?"""

        data_handler = TrainingData(
            project=self._PROJECT,
            gcs_bucket="floracast-datamining",
            name_usage_id="9sykdre6ougztwabsjjufiwvu",
            train_batch_size=20,
            train_epochs=1,
        )

        for p in fetcher.process(0):
            print(p)




if __name__ == '__main__':
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(ExampleFetchTestCase)
    unittest.TextTestRunner().run(suite)