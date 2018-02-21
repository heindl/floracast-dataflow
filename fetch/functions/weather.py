# from __future__ import absolute_import

import apache_beam as beam
from .example import Example, Examples
from multiprocessing.pool import ThreadPool
import astral
import logging
from sklearn.neighbors import BallTree
import numpy
from datetime import datetime, timedelta
from pandas import date_range
from google.cloud import bigquery
import geopy as geopy
from geopy.distance import vincenty

@beam.typehints.with_input_types(beam.typehints.KV[str, beam.typehints.Iterable[Example]])
@beam.typehints.with_output_types(Example)
class FetchWeatherDoFn(beam.DoFn):
    def __init__(self, project):
        super(FetchWeatherDoFn, self).__init__()
        self._fetcher = WeatherFetcher(
            project=project,
            max_station_distance=100,
            weather_days_before=90,
        )

    def process(self, batch):

        logging.debug("Fetching Weather for Batch: %s", batch[0])

        key_components = batch[0].split("-")
        if len(key_components) != 3:
            logging.error("Weather Fetcher received invalid Key [%s]", batch[0])
            return

        year = int(key_components[0])

        if year < 1950 or year > datetime.now().year:
            logging.error("Weather Fetcher received invalid Year [%s]", key_components[0])
            return

        month = int(key_components[1])
        if month < 1 or month > 12:
            logging.error("Weather Fetcher received invalid Month [%s]", key_components[1])

        examples = Examples(list(batch[1]))

        return self._fetcher.process_batch(examples)


class WeatherFetcher:
    def __init__(self, project, max_station_distance, weather_days_before):
        self._project = project
        self._dataset = 'bigquery-public-data:noaa_gsod'
        self._max_weather_station_distance = max_station_distance
        self._weather_days_before = weather_days_before

    def process_batch(self, examples):

        if examples.count() == 0:
            logging.debug("Example batch contains no examples")
            return

        bounds = examples.bounds()
        bounds.extend_radius(self._max_weather_station_distance)

        print("earliest recalc", self._weather_days_before, examples.earliest_datetime() - timedelta(days=self._weather_days_before))

        self._weather_store = WeatherStore(
            project=self._project,
            bounds=bounds,
            earliest_datetime=examples.earliest_datetime() - timedelta(days=self._weather_days_before),
            latest_datetime=examples.latest_datetime()
        )

        pool = ThreadPool(20)
        #
        for example_batch in examples.in_batches(20):

            # for e in example_batch:
            #     print("getting", e.datetime())
            #     r = self._get_record(e)
            #     if r is not None:
            #         yield r
        #
            records = pool.imap_unordered(self._get_record, example_batch)
        #
            for e in records:
                # r = self.get_record(e)
                if e is not None:
                    yield e

                    # with Pool(20) as p:
                    # records = p.map(self.get_record, example_batch)
                    # for e in records:
                    #     # e = r.get()
                    #     if e is not None:
                    #         print("yielding")
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

        for r in records:
            try:
                a = astral.Astral()
                a.solar_depression = 'civil'
                astro = a.sun_utc(
                    datetime.strptime(r["DATE"], '%Y%m%d').date(),
                    example.latitude(),
                    example.longitude()
                )
                day_length = (astro['sunset'] - astro['sunrise']).seconds
            except astral.AstralError as err:
                if "Sun never reaches 6 degrees below the horizon" in err.message:
                    day_length = 86400
                else:
                    logging.error("Error parsing day[%s] length at [%.6f,%.6f]: %s", r["DATE"], example.latitude(), example.longitude(), err)
                    return None

            example.append_daylight(day_length)
            example.append_precipitation(r['PRCP'])
            example.append_temp_min(r['MIN'])
            example.append_temp_max(r['MAX'])
            example.append_temp_avg(r['AVG'])

        return example

RADIANT_TO_KM_CONSTANT = 6367
class BallTreeIndex:
    def __init__(self,lat_longs):
        self.lat_longs = lat_longs
        # print('radians', lat_longs, np.radians(lat_longs))
        self.ball_tree_index = BallTree(numpy.radians(lat_longs), metric='haversine')

    def query_radius(self,lat, lng ,radius_miles):
        # radius_km = radius/1e3
        radius_km = radius_miles * 0.6
        radius_radian = radius_km / RADIANT_TO_KM_CONSTANT
        query = numpy.radians(numpy.array([(lat, lng)]))
        indices = self.ball_tree_index.query_radius(query,r=radius_radian)
        pairs = []
        for i in list(indices)[0]:
            # converted_degrees = np.degrees(self.lat_longs[i])
            # print("converted", converted_degrees)
            pairs.append(
                (
                    vincenty(geopy.Point(lat, lng), geopy.Point(self.lat_longs[i][0], self.lat_longs[i][1])).miles,
                    '%.6f-%.6f' % (self.lat_longs[i][0], self.lat_longs[i][1])
                )
            )
        pairs = sorted(pairs, key=lambda v: v[0])
        return [x[1] for x in pairs][0:5]


class WeatherStore:
    def __init__(self, project, bounds, earliest_datetime, latest_datetime):
        self._project = project
        self._dataset = 'bigquery-public-data:noaa_gsod'
        self._bounds = bounds # swLng, swLat, neLng, neLat
        self._earliest_date = earliest_datetime
        self._latest_date = latest_datetime

        self._position_map = {}
        i = 0
        for d in date_range(
            start=self._earliest_date,
            end=self._latest_date,
            freq='D'
        ).sort_values().tolist():
            d = str(d.strftime("%Y%m%d"))
            self._position_map[d] = i
            i += 1

        self._map = {}
        self._load()

    # year_dictionary provides {2016: ["01"], 2015: [12]}
    def _year_dictionary(self):

        res = {}

        for d in date_range(
            start=self._earliest_date,
            end=self._latest_date,
            freq='M'
        ).tolist():
            if str(d.year) not in res:
                res[str(d.year)] = set()
            res[str(d.year)].add('{:02d}'.format(d.month))

        return res

    def _form_query(self, q_year, q_months):
        month_query = ""
        for m in q_months:
            if month_query == "":
                month_query = "(a.mo = '%s'" % m
            else:
                month_query += " OR a.mo = '%s'" % m
        month_query += ")"
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
            'q_month': month_query
        }

        return q.format(**values)

    def _load(self):

        _client = bigquery.Client(project=self._project)

        station_placeholder = {}
        stations = []

        for year, months in self._year_dictionary().iteritems():
            q = self._form_query(year, months)

            query = _client.query(q)

            # Had some trouble with conflicting versions of this package between local runner and remote.
            #  pip install --upgrade google-cloud-bigquery

            for row in query.result(timeout=30000):

                k = '%.6f-%.6f' % (row[0], row[1])
                if k not in station_placeholder:
                    station_placeholder[k] = 1
                    stations.append((row[0], row[1]))
                    self._map[k] = [None] * len(self._position_map.keys())
                d = str(row[8]+row[6]+row[7])
                if d in self._position_map:
                    self._map[k][self._position_map[d]] = {
                        'PRCP': float(row[2]),
                        'MIN': float(row[3]),
                        'MAX': float(row[4]),
                        'AVG': float(row[5]),
                        'DATE': d,
                    }

        if len(stations) != 0:
            self._stations_index = BallTreeIndex(stations)
        else:
            self._stations_index = None

    def _weather_exists(self, k, start, end):
        if k in self._map:
            for i in range(start, end):
                if self._map[k][i] is None:
                    return False
        return True

    def read(self, lat, lng, today, max_station_distance, days_before):

        if self._stations_index is None:
            return None

        end = self._position_map[today.strftime("%Y%m%d")]
        start = end - days_before

        station_list = self._stations_index.query_radius(lat, lng, max_station_distance)

        for k in station_list:
            if self._weather_exists(k, start, end):
                return self._map[k][start:end]

        return None
