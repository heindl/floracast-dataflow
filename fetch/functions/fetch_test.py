import unittest
from fetch import FetchRandom, FetchOccurrences


class ExampleFetchTestCase(unittest.TestCase):

    _PROJECT = "floracast-firestore"
    _CATEGORY = "AHo2IYxvo37RjezIkho6xBWmq"

    def test_occurrence_fetch(self):
        """Are occurrences correctly fetched?"""

        occurrence_fetcher = FetchOccurrences(project=self._PROJECT)
        occurrences = list(occurrence_fetcher.process("AHo2IYxvo37RjezIkho6xBWmq-|-27-|-2594602"))

        self.assertEqual(len(occurrences), 205)

        example_groups = {}
        for o in occurrences:
            k = o.season_region_key(2)
            if k not in example_groups:
                example_groups[k] = [o]
            else:
                example_groups[k].append(o)

        self.assertEqual(len(example_groups), 111)

        l = []
        for o in example_groups["2017-1-89"]:
            realm, biome, num = o.eco_region()
            l.append({
                "FormattedDate": o.date_string(),
                'GeoFeatureSet': {
                    "GeoPoint": {'latitude': o.latitude(), 'longitude': o.longitude()},
                    "Elevation": o.elevation(),
                    "EcoRealm": realm,
                    "EcoBiome": biome,
                    "EcoNum": num,
                    "S2Tokens": o.s2_tokens(),
                }
            })

    def test_random_fetch(self):
        """Are random points correctly fetched?"""

        random_fetcher = FetchRandom(project=self._PROJECT, should_fetch=True)
        random_points = list(random_fetcher.process(1))

        self.assertEqual(len(random_points), 285)


if __name__ == '__main__':
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(ExampleFetchTestCase)
    unittest.TextTestRunner().run(suite)