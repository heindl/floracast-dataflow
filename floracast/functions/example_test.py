import unittest
from example import ParseExampleFromFirestore, Season
import datetime


class ExampleParseTestCase(unittest.TestCase):

    _CATEGORY = "Io1ftG"

    def test_example_creation_from_firestore(self):
        """Is an example successfully created from a Firestore dictionary?"""

        id = "Io1ftG-27-923908968"
        f = {
            'GeoFeatureSet': {
                'EcoNum': 7,
                'Elevation': 215,
                'SourceType': '27',
                'GeoPoint': {'latitude': 43.736238, 'longitude': -73.210352},
                'TargetID': '2594602',
                'EcoBiome': 4,
                'S2Tokens': {
                    "0": "5",
                    "1": "4c",
                    "2": "4d",
                    "3": "4cc",
                    "4": "4cb",
                    "5": "4ca4",
                    "6": "4ca5",
                    "7": "4ca4c",
                    "8": "4ca4d",
                    "9": "4ca4dc",
                    "10": "4ca4d9",
                    "11": "4ca4d9c",
                    "12": "4ca4d9d",
                    "13": "4ca4d9d4",
                    "14": "4ca4d9d7",
                    "15": "4ca4d9d7c",
                    "16": "4ca4d9d7f"
                },
                'CoordinatesEstimated': False,
                'EcoRealm': 5,
                'SourceOccurrenceID': '923908968'
            },
            'FormattedDate': '20100510',
        }

        ex = ParseExampleFromFirestore(self._CATEGORY, id, f)

        self.assertEqual(ex.example_id(), id)
        self.assertEqual(ex.category(), self._CATEGORY)
        self.assertAlmostEqual(ex.longitude(), -73.2103500)
        self.assertAlmostEqual(ex.latitude(), 43.73623657)
        self.assertEqual(ex.date_string(), '20100510')
        self.assertEqual(ex.datetime().date(), datetime.date(2010, 5, 10))
        self.assertEqual(ex.season().as_int(), Season.Spring)
        self.assertEqual(ex.year(), 2010)
        self.assertEqual(ex.month(), 5)
        self.assertEqual(ex.elevation(), 215)
        self.assertEqual(ex.month_name(), "May")
        self.assertEqual(ex.equality_key(), "%s|||43.7362|||-73.2104|||20100510" % self._CATEGORY)
        self.assertEqual(ex.season_region_key(2), "2010-1-4d")
        self.assertEqual(ex.eco_region(), ('5', '4', '7'))

        for i in f['GeoFeatureSet']["S2Tokens"]:
            self.assertEqual(ex.s2_token(int(i)), f['GeoFeatureSet']["S2Tokens"][i])

if __name__ == '__main__':
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(ExampleParseTestCase)
    unittest.TextTestRunner().run(suite)