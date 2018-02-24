# from __future__ import absolute_import

import apache_beam as beam
from .example import Example, Examples, KEY_PRCP, KEY_DAYLIGHT, KEY_DATE, KEY_AVG_TEMP, KEY_MAX_TEMP, KEY_MIN_TEMP
from multiprocessing.pool import ThreadPool
import astral
import logging
from sklearn.neighbors import BallTree
import datetime
from pandas import date_range
from google.cloud import bigquery
from numpy import array, radians

MINIMUM_YEAR=1950

@beam.typehints.with_input_types(beam.typehints.KV[str, beam.typehints.Iterable[Example]])
@beam.typehints.with_output_types(Example)
class FetchWeatherDoFn(beam.DoFn):
    def __init__(self, project):
        super(FetchWeatherDoFn, self).__init__()
        self._project = project,
        self._max_weather_station_distance = 100 # Kilometers
        self._weather_days_before = 90

    def process(self, batch):

        logging.debug("Fetching Weather for Batch: %s", batch[0])

        key_components = batch[0].split("-")
        if len(key_components) != 3:
            logging.error("Weather Fetcher received invalid Key [%s]", batch[0])
            return

        year = int(key_components[0])

        if year < 1950 or year > datetime.datetime.now().year:
            logging.error("Weather Fetcher received invalid Year [%s]", key_components[0])
            return

        month = int(key_components[1])
        if month < 1 or month > 12:
            logging.error("Weather Fetcher received invalid Month [%s]", key_components[1])

        examples = Examples(list(batch[1]))

        return self._process_batch(examples)

    def _process_batch(self, examples):
        if examples.count() == 0:
            logging.debug("Example batch contains no examples")
            return

        bounds = examples.bounds()
        bounds.extend_radius(self._max_weather_station_distance)

        if type(self._project) is tuple:
            project = self._project[0]
        elif hasattr(self._project, 'get'):
            project = self._project.get()
        else:
            project = self._project

        self._weather_store = WeatherStore(
            project=project,
            bounds=bounds,
            earliest_datetime=examples.earliest_datetime() - datetime.timedelta(days=self._weather_days_before),
            latest_datetime=examples.latest_datetime()
        )

        for e in examples.as_list():
            r = self._get_record(e)
            if r is not None:
                yield r

        # for e in ThreadPool(20).imap_unordered(self._get_record, examples.as_list()):
        #     if e is not None:
        #         yield e

    def _get_record(self, example):

        # Should return in order from least to greatest.
        records = self._weather_store.read(
            example.latitude(),
            example.longitude(),
            example.datetime(),
            self._max_weather_station_distance,
            self._weather_days_before
        )

        if records is None or len(records) < self._weather_days_before:
            return None

        for i, r in enumerate(records):
            example.set_weather(i, r[KEY_AVG_TEMP], r[KEY_MAX_TEMP], r[KEY_MIN_TEMP], r[KEY_PRCP], r[KEY_DAYLIGHT])

        return example


EARTH_RADIUS_KM = 6371.0

class WeatherStore:
    def __init__(self, project, bounds, earliest_datetime, latest_datetime):
        self._project = project
        self._dataset = 'bigquery-public-data:noaa_gsod'
        self._bounds = bounds # swLng, swLat, neLng, neLat
        self._earliest_date = earliest_datetime
        self._latest_date = latest_datetime
        self._station_index = []
        self._station_ball_tree_list = []
        self._astral = astral.Astral()
        self._astral.solar_depression = 'civil'
        self._ball_tree_index = None

        self._index_position_map = {self._date_key(d): i for i, d in enumerate(date_range(
            start=self._earliest_date,
            end=self._latest_date,
            freq='D'
        ).sort_values().to_pydatetime())}

        self._weather_values = {}
        self._load()

    def get_stations(self, lat, lng, radius_km):

        indices, distances = self._ball_tree_index.query_radius(
            # Note that the haversine distance metric requires data in the form of [latitude, longitude]
            # and both inputs and outputs are in units of radians
            radians(array([(lat, lng)])),
            r=(float(radius_km) / float(EARTH_RADIUS_KM)),
            return_distance=True,
            sort_results=True
        )
        if len(indices) == 0:
            return []
        return [self._station_index[i] for i in indices[0]]

    def _form_query(self, q_year, q_months):

        q = """
                  SELECT
                    lat,
                    lon,
                    prcp,
                    min,
                    max,
                    temp,
                    mo,
                    da,
                    year,
                    elev,
                    stn
                  FROM
                    `bigquery-public-data.noaa_gsod.gsod{q_year}` a
                  JOIN
                    `bigquery-public-data.noaa_gsod.stations` b
                  ON
                    a.stn=b.usaf
                    AND a.wban=b.wban
                  WHERE
                     b.lat <= {north} AND b.lat >= {south}
                    AND b.lon >= {west} AND b.lon <= {east}
                    AND {q_month}
                  ORDER BY
                    a.da DESC
            """

        values = {
            'north': str(self._bounds.north()),
            'west': str(self._bounds.west()),
            'south': str(self._bounds.south()),
            'east': str(self._bounds.east()),
            'q_year': q_year,
            'q_month': "(" + " OR ".join(["a.mo = '%s'" % m for m in q_months]) + ")"
        }

        return q.format(**values)

    @staticmethod
    def _date_key(dt):
        if dt.date() < datetime.date(year=MINIMUM_YEAR, month=1, day=1):
            raise ValueError("Invalid Datetime", dt)

        s = dt.strftime("%Y%m%d")
        if dt == "":
            raise ValueError("DateString can not be empty")
        return s

    @staticmethod
    def _location_key(lat, lng):
        if lat == 0 or lng == 0:
            raise ValueError("Location can not be empty")
        return '%.6f|%.6f' % (lat, lng)

    def _parse_row(self, row):
        lat, lng = float(row[0]), float(row[1])
        date_string = str(row[8]+row[6]+row[7])
        key = self._location_key(lat, lng)

        if key not in self._station_index:
            self._station_index.append(key)
            # Note that the haversine distance metric requires data in the form of [latitude, longitude]
            # and both inputs and outputs are in units of radians
            self._station_ball_tree_list.append((lat, lng))
            # Create an empty array with expected number of weather values.
            self._weather_values[key] = [None] * len(self._index_position_map)

        if date_string not in self._index_position_map:
            return

        self._weather_values[key][self._index_position_map[date_string]] = {
            KEY_PRCP: float(row[2]),
            KEY_MIN_TEMP: float(row[3]),
            KEY_MAX_TEMP: float(row[4]),
            KEY_AVG_TEMP: float(row[5]),
            KEY_DATE: date_string,
        }

    def _daylight(self, lat, lng, date_string):
        if len(date_string) != 8:
            raise ValueError("Invalid Date", date_string)
        try:
            a = self._astral.sun_utc(
                datetime.datetime.strptime(date_string, '%Y%m%d').date(),
                lat,
                lng
            )
            return (a['sunset'] - a['sunrise']).seconds
        except astral.AstralError as err:
            if "Sun never reaches 6 degrees below the horizon" in err.message:
                return 86400
            else:
                logging.error("Error parsing day[%s] length at [%.6f,%.6f]: %s", date_string, lat, lng, err)
                return None

    def _load(self):

        bigquery_client = bigquery.Client(project=self._project)


        # {2016: ["01", "02"], 2015: ["12"]}
        for year, months in date_range(
                start=self._earliest_date,
                end=self._latest_date,
                freq='D'
        ).to_series().groupby(lambda x: x.year).apply(lambda x: x.map(lambda d: '{:02d}'.format(d.month)).unique()).items():

            # Had some trouble with conflicting versions of this package between local runner and remote.
            #  pip install --upgrade google-cloud-bigquery
            for row in bigquery_client.query(self._form_query(year, months)).result(timeout=30000):
                self._parse_row(row)

        if len(self._station_ball_tree_list) == 0:
            logging.warn("No stations returned from query", self._earliest_date, self._latest_date)
            return

        self._ball_tree_index = BallTree(
            radians(self._station_ball_tree_list),
            metric='haversine'
        )

    def _all_weather_dates_accounted_for(self, k, start, end):
        if k not in self._weather_values:
            return False
        for i in range(start, end):
            if self._weather_values[k][i] is None:
                return False
        return True

    def read(self, lat, lng, today, max_station_distance_km, days_before):

        if len(self._station_index) == 0:
            return None

        if type(lat) is not float or type(lng) is not float or lat == 0 or lng == 0:
            raise ValueError("Invalid Location for reading weather", lat, lng)

        if today is None or today.date() < datetime.date(year=MINIMUM_YEAR, month=1, day=1):
            raise ValueError("Invalid Datetime for reading Weather", today)

        station_locations = self.get_stations(lat, lng, max_station_distance_km)

        end = self._index_position_map[self._date_key(today)]
        start = end - days_before

        for k in station_locations:
            if self._all_weather_dates_accounted_for(k, start, end) is not True:
                continue

            key_lat = float(k.split("|")[0])
            key_lng = float(k.split("|")[1])
            for i in range(start, end):
                if KEY_DAYLIGHT not in self._weather_values[k][i]:
                    self._weather_values[k][i][KEY_DAYLIGHT] = self._daylight(key_lat, key_lng, self._weather_values[k][i][KEY_DATE])

            return self._weather_values[k][start:end]

        return None
