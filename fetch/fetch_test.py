from functions.fetch import FetchOccurrences
from functions.weather import FetchWeatherDoFn
from functions.example import Examples, ParseExampleFromFirestore
from functions.write import WriteTFRecords
import numpy
PROJECT = "floracast-firestore"
CATEGORY = "AHo2IYxvo37RjezIkho6xBWmq"

# occurrence_fetcher = FetchOccurrences(project=PROJECT)
# occurrences = list(occurrence_fetcher.process("AHo2IYxvo37RjezIkho6xBWmq-|-27-|-2594602"))
#
# example_groups = {}
# for o in occurrences:
#     k = o.season_region_key()
#     if k not in example_groups:
#         example_groups[k] = [o]
#     else:
#         example_groups[k].append(o)
#
# for k in example_groups:
#     print(k, len(example_groups[k]))
#
# l = []
# for o in example_groups["2017-1-89"]:
#     realm, biome, num = o.eco_region()
#     l.append({
#         "FormattedDate": o.date(),
#         "GeoPoint": {'latitude': o.latitude(), 'longitude': o.longitude()},
#         "Elevation": o.elevation(),
#         "EcoRealm": realm,
#         "EcoBiome": biome,
#         "EcoNum": num,
#         "S2Tokens": o.s2_cells(),
#     })


def test_examples():
    test_occurrences = [{'EcoNum': 13, 'Elevation': 217, 'GeoPoint': {'latitude': 35.160975, 'longitude': -81.120388}, 'EcoBiome': 4, 'S2Tokens': ['9', '884', '89', '8854', '885', '8856c', '8857'], 'FormattedDate': '20170406', 'EcoRealm': 5}, {'EcoNum': 13, 'Elevation': 107, 'GeoPoint': {'latitude': 39.058534, 'longitude': -77.025851}, 'EcoBiome': 4, 'S2Tokens': ['9', '89c', '89', '89b4', '89b', '89b7c', '89b7'], 'FormattedDate': '20170408', 'EcoRealm': 5}, {'EcoNum': 4, 'Elevation': 159, 'GeoPoint': {'latitude': 37.51628, 'longitude': -88.755371}, 'EcoBiome': 4, 'S2Tokens': ['9', '884', '89', '8874', '887', '8870c', '8871'], 'FormattedDate': '20170409', 'EcoRealm': 5}, {'EcoNum': 2, 'Elevation': 233, 'GeoPoint': {'latitude': 39.471461, 'longitude': -82.736741}, 'EcoBiome': 4, 'S2Tokens': ['9', '884', '89', '8844', '885', '8847c', '8847'], 'FormattedDate': '20170410', 'EcoRealm': 5}, {'EcoNum': 13, 'Elevation': 69, 'GeoPoint': {'latitude': 38.970464, 'longitude': -77.043931}, 'EcoBiome': 4, 'S2Tokens': ['9', '89c', '89', '89b4', '89b', '89b7c', '89b7'], 'FormattedDate': '20170421', 'EcoRealm': 5}, {'EcoNum': 13, 'Elevation': 75, 'GeoPoint': {'latitude': 38.972144, 'longitude': -77.044236}, 'EcoBiome': 4, 'S2Tokens': ['9', '89c', '89', '89b4', '89b', '89b7c', '89b7'], 'FormattedDate': '20170421', 'EcoRealm': 5}, {'EcoNum': 14, 'Elevation': 354, 'GeoPoint': {'latitude': 40.963669, 'longitude': -81.331219}, 'EcoBiome': 4, 'S2Tokens': ['9', '884', '89', '8834', '883', '88314', '8831'], 'FormattedDate': '20170421', 'EcoRealm': 5}, {'EcoNum': 14, 'Elevation': 312, 'GeoPoint': {'latitude': 39.673882, 'longitude': -83.837375}, 'EcoBiome': 4, 'S2Tokens': ['9', '884', '89', '8844', '885', '8840c', '8841'], 'FormattedDate': '20170422', 'EcoRealm': 5}, {'EcoNum': 14, 'Elevation': 343, 'GeoPoint': {'latitude': 40.993555, 'longitude': -81.798356}, 'EcoBiome': 4, 'S2Tokens': ['9', '884', '89', '8834', '883', '8830c', '8831'], 'FormattedDate': '20170424', 'EcoRealm': 5}, {'EcoNum': 13, 'Elevation': 94, 'GeoPoint': {'latitude': 39.011937, 'longitude': -77.255416}, 'EcoBiome': 4, 'S2Tokens': ['9', '89c', '89', '89b4', '89b', '89b64', '89b7'], 'FormattedDate': '20170420', 'EcoRealm': 5}, {'EcoNum': 14, 'Elevation': 310, 'GeoPoint': {'latitude': 41.279433, 'longitude': -81.374703}, 'EcoBiome': 4, 'S2Tokens': ['9', '884', '89', '8834', '883', '88314', '8831'], 'FormattedDate': '20170430', 'EcoRealm': 5}, {'EcoNum': 4, 'Elevation': 199, 'GeoPoint': {'latitude': 42.132292, 'longitude': -87.795378}, 'EcoBiome': 8, 'S2Tokens': ['9', '884', '89', '880c', '881', '880fc', '880f'], 'FormattedDate': '20170504', 'EcoRealm': 5}, {'EcoNum': 4, 'Elevation': 293, 'GeoPoint': {'latitude': 38.434536, 'longitude': -83.896652}, 'EcoBiome': 4, 'S2Tokens': ['9', '884', '89', '8844', '885', '8843c', '8843'], 'FormattedDate': '20170418', 'EcoRealm': 5}, {'EcoNum': 4, 'Elevation': 222, 'GeoPoint': {'latitude': 40.876463, 'longitude': -89.689117}, 'EcoBiome': 8, 'S2Tokens': ['9', '884', '89', '880c', '881', '880a4', '880b'], 'FormattedDate': '20170420', 'EcoRealm': 5}, {'EcoNum': 14, 'Elevation': 393, 'GeoPoint': {'latitude': 40.700235, 'longitude': -82.55084}, 'EcoBiome': 4, 'S2Tokens': ['9', '884', '89', '883c', '883', '8839c', '8839'], 'FormattedDate': '20170423', 'EcoRealm': 5}, {'EcoNum': 3, 'Elevation': 663, 'GeoPoint': {'latitude': 35.66038, 'longitude': -83.582004}, 'EcoBiome': 4, 'S2Tokens': ['9', '884', '89', '885c', '885', '88594', '8859'], 'FormattedDate': '20170409', 'EcoRealm': 5}, {'EcoNum': 2, 'Elevation': 374, 'GeoPoint': {'latitude': 40.49613, 'longitude': -80.540152}, 'EcoBiome': 4, 'S2Tokens': ['9', '884', '89', '8834', '883', '88344', '8835'], 'FormattedDate': '20170417', 'EcoRealm': 5}, {'EcoNum': 4, 'Elevation': 179, 'GeoPoint': {'latitude': 39.126278, 'longitude': -88.179359}, 'EcoBiome': 8, 'S2Tokens': ['9', '884', '89', '8874', '887', '8873c', '8873'], 'FormattedDate': '20170509', 'EcoRealm': 5}, {'EcoNum': 14, 'Elevation': 184, 'GeoPoint': {'latitude': 43.254393, 'longitude': -81.832781}, 'EcoBiome': 4, 'S2Tokens': ['9', '884', '89', '882c', '883', '882f4', '882f'], 'FormattedDate': '20170519', 'EcoRealm': 5}]
    return Examples([ParseExampleFromFirestore(CATEGORY, str(i), o) for i, o in enumerate(test_occurrences)])

fetcher = FetchWeatherDoFn(project=PROJECT)

ex = test_examples()g

print("STARTING WITH", ex.count())

res = []
for e in fetcher._process_batch(ex):
    if e is not None:
        print("Resolved")
        res.append(e)

print("DONE", len(res))

# local_writer = WriteTFRecords(project=PROJECT, dir="/tmp/occurrence-fetcher-test/")
# print("Written to File", local_writer._write(CATEGORY, Examples(res)))

# remote_writer = WriteTFRecords(project=PROJECT, dir="gs://floracast-datamining/", timestamp="1519243427")
# fname = remote_writer._write(CATEGORY, Examples(res))
# remote_writer._upload(CATEGORY)

