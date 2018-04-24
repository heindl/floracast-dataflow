import unittest

from weather import FetchWeatherDoFn
from example import Examples, ParseExampleFromFirestore


class WeatherFetchTestCase(unittest.TestCase):
    """Tests for `primes.py`."""

    _PROJECT = "floracast-firestore"
    _CATEGORY = "AHo2IYxvo37RjezIkho6xBWmq"

    def test_boundaries(self):
        """Are example list boundaries correctly calculated?"""
        ex_maps = [
            {
                'GeoFeatureSet': {
                    'EcoNum': 13, 'Elevation': 217, 'GeoPoint': {'latitude': 35.160975, 'longitude': -81.120388}, 'EcoBiome': 4, 'S2Tokens': {"0": "5", "1": "4c", "2": "4d", "3": "4cc", "4": "4cb", "5": "4ca4", "6": "4ca5", "7": "4ca4c", "8": "4ca4d", "9": "4ca4dc", "10": "4ca4d9", "11": "4ca4d9c", "12": "4ca4d9d", "13": "4ca4d9d4", "14": "4ca4d9d7", "15": "4ca4d9d7c", "16": "4ca4d9d7f"}, 'EcoRealm': 5 },
                'FormattedDate': '20170406'
            },
            {
                'GeoFeatureSet': {
                    'EcoNum': 13, 'Elevation': 107, 'GeoPoint': {'latitude': 39.058534, 'longitude': -77.025851}, 'EcoBiome': 4, 'S2Tokens': {"0": "5", "1": "4c", "2": "4d", "3": "4cc", "4": "4cb", "5": "4ca4", "6": "4ca5", "7": "4ca4c", "8": "4ca4d", "9": "4ca4dc", "10": "4ca4d9", "11": "4ca4d9c", "12": "4ca4d9d", "13": "4ca4d9d4", "14": "4ca4d9d7", "15": "4ca4d9d7c", "16": "4ca4d9d7f"}, 'EcoRealm': 5 },
                'FormattedDate': '20170408'
            }
        ]
        res = Examples([ParseExampleFromFirestore(self._CATEGORY, str(i), o) for i, o in enumerate(ex_maps)])

        bounds = res.bounds()
        # 39.058534, -77.025851
        # 35.160975, -81.120388
        self.assertAlmostEqual(bounds.east(), -75.7065721)
        self.assertAlmostEqual(bounds.west(), -82.0484663)
        self.assertAlmostEqual(bounds.north(), 39.4868029) # 39.486803781445325, -75.70657312410647
        self.assertAlmostEqual(bounds.south(), 34.3943273)

        bounds.extend_radius(100)
        self.assertAlmostEqual(bounds.east(), -74.5857196)
        self.assertAlmostEqual(bounds.west(), -83.1693187)
        self.assertAlmostEqual(bounds.north(), 40.3874328) # 40.38743363859794, -74.58572064003887
        self.assertAlmostEqual(bounds.south(), 33.4927875) # 33.49278568783416, -74.58572064003887




    def test_weather_fetch(self):
        ex_maps = [
            {'GeoFeatureSet': {'EcoNum': 7, 'Elevation': 76, 'SourceType': '27', 'GeoPoint': {'latitude': 46.881808, 'longitude': -71.17635}, 'TargetID': '2594602', 'S2Tokens': {
                "0": "5", "1": "4c", "2": "4d", "3": "4cc", "4": "4cb", "5": "4ca4", "6": "4ca5", "7": "4ca4c", "8": "4ca4d", "9": "4ca4dc", "10": "4ca4d9", "11": "4ca4d9c", "12": "4ca4d9d", "13": "4ca4d9d4", "14": "4ca4d9d7", "15": "4ca4d9d7c", "16": "4ca4d9d7f"
            }, 'EcoBiome': 4, 'SourceOccurrenceID': '1135069942', 'EcoRealm': 5, 'CoordinatesEstimated': False}, 'FormattedDate': '20060507'},
          ]
        examples = Examples([ParseExampleFromFirestore(self._CATEGORY, str(i), o) for i, o in enumerate(ex_maps)])

        fn = FetchWeatherDoFn(self._PROJECT)
        res = list(fn._process_batch(examples))
        # self.assertEqual(len(res), 3)
        for e in res:
            min_array = e.get_min_temp()
            self.assertEqual(len(min_array), 120)
            max_array = e.get_max_temp()
            self.assertEqual(len(max_array), 120)
            for a, min in enumerate(min_array):
                self.assertGreater(max_array[a], min, "MinTemp [%d] is less than MaxTemp [%d] at Position [%d]" % (min, max_array[a], a))




if __name__ == '__main__':
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(WeatherFetchTestCase)
    unittest.TextTestRunner().run(suite)