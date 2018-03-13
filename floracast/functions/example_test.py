import unittest
from example import ParseExampleFromFirestore, Season
import datetime


class ExampleParseTestCase(unittest.TestCase):

    _CATEGORY = "AHo2IYxvo37RjezIkho6xBWmq"

    def test_example_creation_from_firestore(self):
        """Is an example successfully created from a Firestore dictionary?"""

        id = "AHo2IYxvo37RjezIkho6xBWmq-27-923908968"
        f = {
            'GeoFeatureSet': {
                'CoordinateKey': '43.736|-73.210',
                'EcoNum': 7,
                'Elevation': 215,
                'SourceType': '27',
                'GeoPoint': {'latitude': 43.736238, 'longitude': -73.210352},
                'TargetID': '2594602',
                'EcoBiome': 4,
                'S2Tokens': {'1': '8c', '0': '9', '3': '89c', '2': '89', '5': '89e4', '4': '89f', '7': '89e04', '6': '89e1'},
                'CoordinatesEstimated': False,
                'EcoRealm': 5,
                'SourceOccurrenceID': '923908968'
            },
            'FormattedDate': '20100510',
        }

        ex = ParseExampleFromFirestore(self._CATEGORY, id, f)

        self.assertEqual(ex.example_id(), id)
        self.assertEqual(ex.category(), self._CATEGORY)
        self.assertEqual(ex.longitude(), -73.210352)
        self.assertEqual(ex.latitude(), 43.736238)
        self.assertEqual(ex.date_string(), '20100510')
        self.assertEqual(ex.datetime().date(), datetime.date(2010, 5, 10))
        self.assertEqual(ex.season().as_int(), Season.Spring)
        self.assertEqual(ex.year(), 2010)
        self.assertEqual(ex.month(), 5)
        self.assertEqual(ex.elevation(), 215)
        self.assertEqual(ex.month_name(), "May")
        self.assertEqual(ex.equality_key(), "%s|||43.7362|||-73.2104|||20100510" % self._CATEGORY)
        self.assertEqual(ex.season_region_key(2), "2010-1-89")
        self.assertEqual(ex.eco_region(), ('5', '4', '7'))

        for i in f['GeoFeatureSet']["S2Tokens"]:
            self.assertEqual(ex.s2_token(int(i)), f['GeoFeatureSet']["S2Tokens"][i])

if __name__ == '__main__':
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(ExampleParseTestCase)
    unittest.TextTestRunner().run(suite)