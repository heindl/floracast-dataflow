from functions.fetch import FetchOccurrences
from functions.weather import WeatherFetcher
from functions.example import Examples
from functions.write import WriteTFRecords

# logging.getLogger().setLevel(logging.DEBUG)

PROJECT = "floracast-firestore"
CATEGORY = "AHo2IYxvo37RjezIkho6xBWmq"

# occurrence_fetcher = FetchOccurrences(project=PROJECT)
# occurrences = list(occurrence_fetcher.process("AHo2IYxvo37RjezIkho6xBWmq-|-27-|-2594602"))
#
# example_groups = {}
# for o in occurrences:
#     k = o.season_key()
#     if k not in example_groups:
#         example_groups[k] = [o]
#     else:
#         example_groups[k].append(o)
#
# for k in example_groups:
#     print(k, len(example_groups[k]))
#
# for o in example_groups["2017-1-89"]:
#     print(o.datetime())
#
# fetcher = WeatherFetcher(project=PROJECT, max_station_distance=100, weather_days_before=90)
#
# res = []
# for e in fetcher.process_batch(Examples(example_groups["2017-1-89"])):
#     print("Element resolved")
#     res.append(e)
#
# local_writer = WriteTFRecords(project=PROJECT, dir="/tmp/occurrence-fetcher-test/")
# print("Written to File", local_writer._write(CATEGORY, Examples(res)))

remote_writer = WriteTFRecords(project=PROJECT, dir="gs://floracast-datamining/", timestamp="1519243427")
# fname = remote_writer._write(CATEGORY, Examples(res))
remote_writer._upload(CATEGORY)

