import unittest

from functions.fetch import FetchOccurrences
from functions.weather import FetchWeatherDoFn
from functions.example import Examples, ParseExampleFromFirestore, Season
from functions.write import WriteTFRecords
import datetime
import numpy


class ExampleFetchTestCase(unittest.TestCase):
    """Tests for `primes.py`."""

    _PROJECT = "floracast-firestore"
    _CATEGORY = "AHo2IYxvo37RjezIkho6xBWmq"

    def test_example_creation_from_firestore(self):
        """Is an example successfully created from a Firestore dictionary?"""

        id = "AHo2IYxvo37RjezIkho6xBWmq-27-923908968"
        f = {
             'CoordinateKey': '43.736|-73.210',
             'EcoNum': 7,
             'Elevation': 215,
             'SourceType': '27',
             'GeoPoint': {'latitude': 43.736238, 'longitude': -73.210352},
             'TargetID': '2594602',
             'EcoBiome': 4,
             'S2Tokens': {'1': '8c', '0': '9', '3': '89c', '2': '89', '5': '89e4', '4': '89f', '7': '89e04', '6': '89e1'},
             'CoordinatesEstimated': False,
             'FormattedDate': '20100510',
             'EcoRealm': 5,
             'SourceOccurrenceID': '923908968'
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
        self.assertEqual(ex.eco_region(), (5, 4, 7))

        for i in f["S2Tokens"]:
            self.assertEqual(ex.s2_token(int(i)), f["S2Tokens"][i])

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
                "GeoPoint": {'latitude': o.latitude(), 'longitude': o.longitude()},
                "Elevation": o.elevation(),
                "EcoRealm": realm,
                "EcoBiome": biome,
                "EcoNum": num,
                "S2Tokens": o.s2_tokens(),
            })

        print(l)

    def test_boundaries(self):
        """Are example list boundaries correctly calculated?"""
        ex_maps = [
            {'EcoNum': 13, 'Elevation': 217, 'GeoPoint': {'latitude': 35.160975, 'longitude': -81.120388}, 'EcoBiome': 4, 'S2Tokens': ['9', '8c', '89', '884', '885', '8854', '8857', '8856c'], 'FormattedDate': '20170406', 'EcoRealm': 5},
            {'EcoNum': 13, 'Elevation': 107, 'GeoPoint': {'latitude': 39.058534, 'longitude': -77.025851}, 'EcoBiome': 4, 'S2Tokens': ['9', '8c', '89', '89c', '89b', '89b4', '89b7', '89b7c'], 'FormattedDate': '20170408', 'EcoRealm': 5}
        ]
        res = Examples([ParseExampleFromFirestore(self._CATEGORY, str(i), o) for i, o in enumerate(ex_maps)])

        bounds = res.bounds()
        # 39.058534, -77.025851
        # 35.160975, -81.120388
        self.assertEqual(bounds.east(), -75.70657312410647)
        self.assertEqual(bounds.west(), -82.04847051363451)
        self.assertEqual(bounds.north(), 39.486803781445325) # 39.486803781445325, -75.70657312410647
        self.assertEqual(bounds.south(), 34.394325474393106)

        bounds.extend_radius(100)
        self.assertEqual(bounds.east(), -74.58572064003887)
        self.assertEqual(bounds.west(), -83.16932299770211)
        self.assertEqual(bounds.north(), 40.38743363859794) # 40.38743363859794, -74.58572064003887
        self.assertEqual(bounds.south(), 33.49278568783416) # 33.49278568783416, -74.58572064003887

    def test_weather_fetch(self):
        ex_maps = [
            {'CoordinateKey': '46.882|-71.176', 'EcoNum': 7, 'Elevation': 76, 'SourceType': '27', 'GeoPoint': {'latitude': 46.881808, 'longitude': -71.17635}, 'TargetID': '2594602', 'S2Tokens': {'1': '4c', '0': '5', '3': '4cc', '2': '4d', '5': '4cbc', '4': '4cb', '7': '4cb8c', '6': '4cb9'}, 'EcoBiome': 4, 'SourceOccurrenceID': '1135069942', 'FormattedDate': '20060507', 'EcoRealm': 5, 'CoordinatesEstimated': False},
            # {'CoordinateKey': '46.900|-71.179', 'EcoNum': 6, 'Elevation': 127, 'SourceType': '27', 'GeoPoint': {'latitude': 46.899638, 'longitude': -71.178526}, 'TargetID': '2594602', 'S2Tokens': {'1': '4c', '0': '5', '3': '4cc', '2': '4d', '5': '4cbc', '4': '4cb', '7': '4cb8c', '6': '4cb9'}, 'EcoBiome': 4, 'CoordinatesEstimated': False, 'SourceOccurrenceID': '1135069967', 'EcoRealm': 5, 'FormattedDate': '20060522'},
            # {'CoordinateKey': '46.882|-71.176', 'EcoNum': 7, 'Elevation': 76, 'SourceType': '27', 'GeoPoint': {'latitude': 46.881808, 'longitude': -71.17635}, 'TargetID': '2594602', 'S2Tokens': {'1': '4c', '0': '5', '3': '4cc', '2': '4d', '5': '4cbc', '4': '4cb', '7': '4cb8c', '6': '4cb9'}, 'EcoBiome': 4, 'SourceOccurrenceID': '1135070017', 'FormattedDate': '20060522', 'EcoRealm': 5, 'CoordinatesEstimated': False}
        ]
        examples = Examples([ParseExampleFromFirestore(self._CATEGORY, str(i), o) for i, o in enumerate(ex_maps)])

        fn = FetchWeatherDoFn(self._PROJECT)
        res = list(fn._process_batch(examples))
        # self.assertEqual(len(res), 3)
        for e in res:
            min_array = e.get_min_temp()
            print(min_array)
            self.assertEqual(len(min_array), 90)
            max_array = e.get_max_temp()
            print(max_array)
            self.assertEqual(len(max_array), 90)
            for a, min in enumerate(min_array):
                self.assertGreater(max_array[a], min, "MinTemp [%d] is less than MaxTemp [%d] at Position [%d]" % (min, max_array[a], a))




#
# def test_examples():
#     test_occurrences = [{'EcoNum': 13, 'Elevation': 217, 'GeoPoint': {'latitude': 35.160975, 'longitude': -81.120388}, 'EcoBiome': 4, 'S2Tokens': ['9', '884', '89', '8854', '885', '8856c', '8857'], 'FormattedDate': '20170406', 'EcoRealm': 5}, {'EcoNum': 13, 'Elevation': 107, 'GeoPoint': {'latitude': 39.058534, 'longitude': -77.025851}, 'EcoBiome': 4, 'S2Tokens': ['9', '89c', '89', '89b4', '89b', '89b7c', '89b7'], 'FormattedDate': '20170408', 'EcoRealm': 5}, {'EcoNum': 4, 'Elevation': 159, 'GeoPoint': {'latitude': 37.51628, 'longitude': -88.755371}, 'EcoBiome': 4, 'S2Tokens': ['9', '884', '89', '8874', '887', '8870c', '8871'], 'FormattedDate': '20170409', 'EcoRealm': 5}, {'EcoNum': 2, 'Elevation': 233, 'GeoPoint': {'latitude': 39.471461, 'longitude': -82.736741}, 'EcoBiome': 4, 'S2Tokens': ['9', '884', '89', '8844', '885', '8847c', '8847'], 'FormattedDate': '20170410', 'EcoRealm': 5}, {'EcoNum': 13, 'Elevation': 69, 'GeoPoint': {'latitude': 38.970464, 'longitude': -77.043931}, 'EcoBiome': 4, 'S2Tokens': ['9', '89c', '89', '89b4', '89b', '89b7c', '89b7'], 'FormattedDate': '20170421', 'EcoRealm': 5}, {'EcoNum': 13, 'Elevation': 75, 'GeoPoint': {'latitude': 38.972144, 'longitude': -77.044236}, 'EcoBiome': 4, 'S2Tokens': ['9', '89c', '89', '89b4', '89b', '89b7c', '89b7'], 'FormattedDate': '20170421', 'EcoRealm': 5}, {'EcoNum': 14, 'Elevation': 354, 'GeoPoint': {'latitude': 40.963669, 'longitude': -81.331219}, 'EcoBiome': 4, 'S2Tokens': ['9', '884', '89', '8834', '883', '88314', '8831'], 'FormattedDate': '20170421', 'EcoRealm': 5}, {'EcoNum': 14, 'Elevation': 312, 'GeoPoint': {'latitude': 39.673882, 'longitude': -83.837375}, 'EcoBiome': 4, 'S2Tokens': ['9', '884', '89', '8844', '885', '8840c', '8841'], 'FormattedDate': '20170422', 'EcoRealm': 5}, {'EcoNum': 14, 'Elevation': 343, 'GeoPoint': {'latitude': 40.993555, 'longitude': -81.798356}, 'EcoBiome': 4, 'S2Tokens': ['9', '884', '89', '8834', '883', '8830c', '8831'], 'FormattedDate': '20170424', 'EcoRealm': 5}, {'EcoNum': 13, 'Elevation': 94, 'GeoPoint': {'latitude': 39.011937, 'longitude': -77.255416}, 'EcoBiome': 4, 'S2Tokens': ['9', '89c', '89', '89b4', '89b', '89b64', '89b7'], 'FormattedDate': '20170420', 'EcoRealm': 5}, {'EcoNum': 14, 'Elevation': 310, 'GeoPoint': {'latitude': 41.279433, 'longitude': -81.374703}, 'EcoBiome': 4, 'S2Tokens': ['9', '884', '89', '8834', '883', '88314', '8831'], 'FormattedDate': '20170430', 'EcoRealm': 5}, {'EcoNum': 4, 'Elevation': 199, 'GeoPoint': {'latitude': 42.132292, 'longitude': -87.795378}, 'EcoBiome': 8, 'S2Tokens': ['9', '884', '89', '880c', '881', '880fc', '880f'], 'FormattedDate': '20170504', 'EcoRealm': 5}, {'EcoNum': 4, 'Elevation': 293, 'GeoPoint': {'latitude': 38.434536, 'longitude': -83.896652}, 'EcoBiome': 4, 'S2Tokens': ['9', '884', '89', '8844', '885', '8843c', '8843'], 'FormattedDate': '20170418', 'EcoRealm': 5}, {'EcoNum': 4, 'Elevation': 222, 'GeoPoint': {'latitude': 40.876463, 'longitude': -89.689117}, 'EcoBiome': 8, 'S2Tokens': ['9', '884', '89', '880c', '881', '880a4', '880b'], 'FormattedDate': '20170420', 'EcoRealm': 5}, {'EcoNum': 14, 'Elevation': 393, 'GeoPoint': {'latitude': 40.700235, 'longitude': -82.55084}, 'EcoBiome': 4, 'S2Tokens': ['9', '884', '89', '883c', '883', '8839c', '8839'], 'FormattedDate': '20170423', 'EcoRealm': 5}, {'EcoNum': 3, 'Elevation': 663, 'GeoPoint': {'latitude': 35.66038, 'longitude': -83.582004}, 'EcoBiome': 4, 'S2Tokens': ['9', '884', '89', '885c', '885', '88594', '8859'], 'FormattedDate': '20170409', 'EcoRealm': 5}, {'EcoNum': 2, 'Elevation': 374, 'GeoPoint': {'latitude': 40.49613, 'longitude': -80.540152}, 'EcoBiome': 4, 'S2Tokens': ['9', '884', '89', '8834', '883', '88344', '8835'], 'FormattedDate': '20170417', 'EcoRealm': 5}, {'EcoNum': 4, 'Elevation': 179, 'GeoPoint': {'latitude': 39.126278, 'longitude': -88.179359}, 'EcoBiome': 8, 'S2Tokens': ['9', '884', '89', '8874', '887', '8873c', '8873'], 'FormattedDate': '20170509', 'EcoRealm': 5}, {'EcoNum': 14, 'Elevation': 184, 'GeoPoint': {'latitude': 43.254393, 'longitude': -81.832781}, 'EcoBiome': 4, 'S2Tokens': ['9', '884', '89', '882c', '883', '882f4', '882f'], 'FormattedDate': '20170519', 'EcoRealm': 5}]
#     return Examples([ParseExampleFromFirestore(CATEGORY, str(i), o) for i, o in enumerate(test_occurrences)])
#
# fetcher = FetchWeatherDoFn(project=PROJECT)
#
# ex = test_examples()g
#
# print("STARTING WITH", ex.count())
#
# res = []
# for e in fetcher._process_batch(ex):
#     if e is not None:
#         print("Resolved")
#         res.append(e)
#
# print("DONE", len(res))

# local_writer = WriteTFRecords(project=PROJECT, dir="/tmp/occurrence-fetcher-test/")
# print("Written to File", local_writer._write(CATEGORY, Examples(res)))

# remote_writer = WriteTFRecords(project=PROJECT, dir="gs://floracast-datamining/", timestamp="1519243427")
# fname = remote_writer._write(CATEGORY, Examples(res))
# remote_writer._upload(CATEGORY)

if __name__ == '__main__':
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(ExampleFetchTestCase)
    unittest.TextTestRunner().run(suite)