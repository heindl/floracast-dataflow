from fetchers.shared.shared.weather import _WeatherLoader
import datetime

loader = _WeatherLoader(
    project="floracast-firestore",
    bbox=[-86.3861256,32.3437215,-81.1200246,35.2028307],
    year=2016,
    month=6,
    weather_station_distance=100,
    days_before=90
)

print(loader.read(33.8184206,-84.5799149, datetime.datetime(2016, 6, 15)))