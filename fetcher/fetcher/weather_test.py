from weather import _WeatherLoader
import datetime

loader = _WeatherLoader(
    project="floracast-firestore",
    bbox=[-85.0795,33.2398,-83.3327,34.1448],
    year=2016,
    month=6,
    weather_station_distance=100)

print(loader.read(33.8184206,-84.5799149, datetime.datetime(2016, 6, 15)))