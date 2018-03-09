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
                    'EcoNum': 13, 'Elevation': 217, 'GeoPoint': {'latitude': 35.160975, 'longitude': -81.120388}, 'EcoBiome': 4, 'S2Tokens': ['9', '8c', '89', '884', '885', '8854', '8857', '8856c'], 'EcoRealm': 5 },
                'FormattedDate': '20170406'
            },
            {
                'GeoFeatureSet': {
                    'EcoNum': 13, 'Elevation': 107, 'GeoPoint': {'latitude': 39.058534, 'longitude': -77.025851}, 'EcoBiome': 4, 'S2Tokens': ['9', '8c', '89', '89c', '89b', '89b4', '89b7', '89b7c'], 'EcoRealm': 5 },
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
            {'GeoFeatureSet': {'CoordinateKey': '46.882|-71.176', 'EcoNum': 7, 'Elevation': 76, 'SourceType': '27', 'GeoPoint': {'latitude': 46.881808, 'longitude': -71.17635}, 'TargetID': '2594602', 'S2Tokens': {'1': '4c', '0': '5', '3': '4cc', '2': '4d', '5': '4cbc', '4': '4cb', '7': '4cb8c', '6': '4cb9'}, 'EcoBiome': 4, 'SourceOccurrenceID': '1135069942', 'EcoRealm': 5, 'CoordinatesEstimated': False}, 'FormattedDate': '20060507'},
            # {'CoordinateKey': '46.900|-71.179', 'EcoNum': 6, 'Elevation': 127, 'SourceType': '27', 'GeoPoint': {'latitude': 46.899638, 'longitude': -71.178526}, 'TargetID': '2594602', 'S2Tokens': {'1': '4c', '0': '5', '3': '4cc', '2': '4d', '5': '4cbc', '4': '4cb', '7': '4cb8c', '6': '4cb9'}, 'EcoBiome': 4, 'CoordinatesEstimated': False, 'SourceOccurrenceID': '1135069967', 'EcoRealm': 5, 'FormattedDate': '20060522'},
            # {'CoordinateKey': '46.882|-71.176', 'EcoNum': 7, 'Elevation': 76, 'SourceType': '27', 'GeoPoint': {'latitude': 46.881808, 'longitude': -71.17635}, 'TargetID': '2594602', 'S2Tokens': {'1': '4c', '0': '5', '3': '4cc', '2': '4d', '5': '4cbc', '4': '4cb', '7': '4cb8c', '6': '4cb9'}, 'EcoBiome': 4, 'SourceOccurrenceID': '1135070017', 'FormattedDate': '20060522', 'EcoRealm': 5, 'CoordinatesEstimated': False}
        ]
        examples = Examples([ParseExampleFromFirestore(self._CATEGORY, str(i), o) for i, o in enumerate(ex_maps)])

        fn = FetchWeatherDoFn(self._PROJECT)
        res = list(fn._process_batch(examples))
        # self.assertEqual(len(res), 3)
        for e in res:
            min_array = e.get_min_temp()
            self.assertEqual(len(min_array), 90)
            max_array = e.get_max_temp()
            self.assertEqual(len(max_array), 90)
            for a, min in enumerate(min_array):
                self.assertGreater(max_array[a], min, "MinTemp [%d] is less than MaxTemp [%d] at Position [%d]" % (min, max_array[a], a))




if __name__ == '__main__':
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(WeatherFetchTestCase)
    unittest.TextTestRunner().run(suite)