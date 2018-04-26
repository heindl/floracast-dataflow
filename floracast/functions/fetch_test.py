import unittest
from write import ExampleRecordWriter
from datetime import datetime
from occurrences import OccurrenceTFRecords
from os import path
import fetch


class ExampleFetchTestCase(unittest.TestCase):

    _PROJECT = "floracast-firestore"
    # _CATEGORY = "AHo2IYxvo37RjezIkho6xBWmq"

    # def test_name_usage_fetch(self):
    #     usageFetcher = FetchNameUsages(project=self._PROJECT, nameusages="2xUhop2,8vMzLmz,BL6T9EP")
    #     sources = list(usageFetcher.process())
    #     self.assertEqual(len(sources), 17)

    # def test_generator(self):
    #     generator = fetch.GeneratePointBatches(project=self._PROJECT, collection="ProtectedAreas", engage=True)
    #
    #     l = list(generator.process(0))
    #     self.assertEqual(len(l), 17)

    # def test_occurrence_fetch(self):
    #     """Are occurrences correctly fetched?"""
    #
    #     occurrence_fetcher = FetchOccurrences(project=self._PROJECT)
    #     occurrences = list(occurrence_fetcher.process("ugkG3de-|-27-|-2526530"))
    #
    #     self.assertEqual(len(occurrences), 829)
    #
    #     example_groups = {}
    #     for o in occurrences:
    #         k = o.season_region_key(2)
    #         if k not in example_groups:
    #             example_groups[k] = [o]
    #         else:
    #             example_groups[k].append(o)
    #
    #     self.assertEqual(len(example_groups), 232)

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
    def test_protected_area(self):

        generated = [[1524709454106576809L, 1524709500681148827L], [1524709500681148827L, 1524709543101281776L], [1524709543101281776L, 1524709583649974355L], [1524709583649974355L, 1524709626241070371L], [1524709626241070371L, 1524709667218791883L], [1524709667218791883L, 1524709709006832435L], [1524709709006832435L, 1524709753325271926L], [1524709753325271926L, 1524709793774425660L], [1524709793774425660L, 1524709838484958710L], [1524709838484958710L, 1524709882179012422L], [1524709882179012422L, 1524709925496147411L], [1524709925496147411L, 1524709967040278683L], [1524709967040278683L, 1524710007745831715L], [1524710007745831715L, 1524710048097675428L], [1524710048097675428L, 1524710089237106967L], [1524710089237106967L, 1524710129613449631L], [1524710129613449631L, 0]]
        total = 0
        for g in generated:
            area_fetcher = fetch.FetchProtectedAreas(project=self._PROJECT)
            total = total + len(list(area_fetcher.process(g)))

        self.assertEqual(total, 8294)



if __name__ == '__main__':
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(ExampleFetchTestCase)
    unittest.TextTestRunner().run(suite)