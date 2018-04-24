import unittest
from fetch import FetchRandom, FetchOccurrences, GenerateProtectedAreaBatches, FetchProtectedAreas, FetchNameUsages
from write import ExampleRecordWriter
from datetime import datetime
from occurrences import OccurrenceTFRecords
from os import path


class ExampleFetchTestCase(unittest.TestCase):

    _PROJECT = "floracast-firestore"
    # _CATEGORY = "AHo2IYxvo37RjezIkho6xBWmq"

    def test_name_usage_fetch(self):
        usageFetcher = FetchNameUsages(project=self._PROJECT, nameusages="2xUhop2,8vMzLmz,BL6T9EP")
        sources = list(usageFetcher.process())
        self.assertEqual(len(sources), 17)


    def test_occurrence_fetch(self):
        """Are occurrences correctly fetched?"""

        occurrence_fetcher = FetchOccurrences(project=self._PROJECT)
        occurrences = list(occurrence_fetcher.process("ugkG3de-|-27-|-2526530"))

        self.assertEqual(len(occurrences), 829)

        example_groups = {}
        for o in occurrences:
            k = o.season_region_key(2)
            if k not in example_groups:
                example_groups[k] = [o]
            else:
                example_groups[k].append(o)

        self.assertEqual(len(example_groups), 232)

        # l = []
        # for o in example_groups["2017-1-89"]:
        #     realm, biome, num = o.eco_region()
        #     l.append({
        #         "FormattedDate": o.date_string(),
        #         'GeoFeatureSet': {
        #             "GeoPoint": {'latitude': o.latitude(), 'longitude': o.longitude()},
        #             "Elevation": o.elevation(),
        #             "EcoRealm": realm,
        #             "EcoBiome": biome,
        #             "EcoNum": num,
        #             "S2Tokens": o.s2_tokens(),
        #         }
        #     })

    # def test_random_fetch(self):
    #     """Are random points correctly fetched?"""
    #
    #     random_fetcher = FetchRandom(project=self._PROJECT, should_fetch=True)
    #     random_points = list(random_fetcher.process(1))
    #
    #     self.assertEqual(len(random_points), 635)
    #
    #     example_groups = {}
    #     for o in random_points:
    #         k = o.pipeline_category()
    #         if k not in example_groups:
    #             example_groups[k] = [o]
    #         else:
    #             example_groups[k].append(o)
    #
    #     self.assertEqual(len(example_groups), 3)
    #
    #     ts = datetime.now().strftime("%s")
    #
    #     for k in example_groups:
    #         print(k)
    #
    #     writer = ExampleRecordWriter(project=self._PROJECT, timestamp=ts)
    #
    #     for k in example_groups:
    #         writer.process((k, example_groups[k]))
    #
    #     f = path.join(writer._local_dir, "random", ts, "*.tfrecords")
    #
    #     self.assertEqual(OccurrenceTFRecords.count(f), 635)
    #
    # def test_protected_area(self):
    #
    #     batcher = GenerateProtectedAreaBatches(project=self._PROJECT, protected_area_dates="20060102")
    #     print(list(batcher.process(0)))
    #
    #     area_fetcher = FetchProtectedAreas(project=self._PROJECT)
    #     areas = list(area_fetcher.process(100))
    #     print("FetchAreas", len(areas))



if __name__ == '__main__':
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(ExampleFetchTestCase)
    unittest.TextTestRunner().run(suite)