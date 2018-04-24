import unittest
from transform import FetchExampleFiles


class ExampleFetchTestCase(unittest.TestCase):

    _PROJECT = "floracast-firestore"

    def test_transform_file_fetcher(self):
        """Are random points correctly fetched?"""
        #
        # fetcher = FetchExampleFiles(project=self._PROJECT, bucket="floracast-datamining")
        #
        # for p in fetcher.process(0):
        #     print(p)




if __name__ == '__main__':
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(ExampleFetchTestCase)
    unittest.TextTestRunner().run(suite)