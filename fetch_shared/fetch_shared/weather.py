# from __future__ import absolute_import
import apache_beam as beam

from ex import Example

@beam.typehints.with_input_types(beam.typehints.Tuple[str, beam.typehints.Iterable[Example]])
@beam.typehints.with_output_types(Example)
class FetchWeatherDoFn(beam.DoFn):
    def __init__(self, project, min_weather_station_distance):
        super(FetchWeatherDoFn, self).__init__()
        self._project = project
        self._weather_station_distance = min_weather_station_distance

    def process(self, batch):

        if type(self._weather_station_distance) is not int:
            self._weather_station_distance = self._weather_station_distance.get()

        fetcher = WeatherFetcher(self._project, self._weather_station_distance)
        return fetcher.process_batch(batch)


class WeatherFetcher:
    def __init__(self, project, min_weather_station_distance):
        self._project = project
        self._dataset = 'bigquery-public-data:noaa_gsod'
        self._weather_station_distance = min_weather_station_distance
        self._weather_days_before = 90

    def process_batch(self, batch):
        from multiprocessing.pool import ThreadPool
        # import time

        # First identify a bounding box.
        # [33.2398, -85.0795], [34.1448, -83.3327]
        sw, ne = [0, 0], [0, 0]

        examples = list(batch[1])

        for example in examples:
            if sw[0] == 0 or sw[0] > example.latitude():
                sw[0] = example.latitude()
            if sw[1] == 0 or sw[1] > example.longitude():
                sw[1] = example.longitude()

            if ne[0] == 0 or ne[0] < example.latitude():
                ne[0] = example.latitude()
            if ne[1] == 0 or ne[1] < example.longitude():
                ne[1] = example.longitude()

        # For each point, ensure we give enough space to get a station for that furthest point.
        sw = self.bounding_box(sw[0], sw[1], self._weather_station_distance)[0]
        ne = self.bounding_box(ne[0], ne[1], self._weather_station_distance)[1]

        self._weather_store = _WeatherLoader(
            project=self._project,
            bbox=[sw.longitude, sw.latitude, ne.longitude, ne.latitude],
            year=batch[0][0:4],
            month=batch[0][4:6],
            weather_station_distance=self._weather_station_distance,
            days_before=self._weather_days_before
        )

        for example_batch in self._chunks(examples, 250):

            records = ThreadPool(50).imap_unordered(self.get_record, example_batch)

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


    def _chunks(self, examples, n):
        """Yield successive n-sized chunks from l."""
        for i in xrange(0, len(examples), n):
            yield examples[i:i + n]

    def get_record(self, example):

        import astral
        import logging
        from datetime import datetime

        records = self._weather_store.read(example.latitude(), example.longitude(), datetime.fromtimestamp(example.date()))

        if len(records.keys()) < self._weather_days_before:
            return None

        for date_string in sorted(records.keys()):
            try:
                a = astral.Astral()
                a.solar_depression = 'civil'
                astro = a.sun_utc(
                    datetime.strptime(date_string, '%Y%m%d').date(),
                    example.latitude(),
                    example.longitude()
                )
                day_length = (astro['sunset'] - astro['sunrise']).seconds
            except astral.AstralError as err:
                if "Sun never reaches 6 degrees below the horizon" in err.message:
                    day_length = 86400
                else:
                    logging.error("Error parsing day[%s] length at [%.6f,%.6f]: %s", date_string, example.latitude(), example.longitude(), err)
                    return None

            example.append_daylight(day_length)
            example.append_precipitation(float(records[date_string]['PRCP']))
            example.append_temp_min(float(records[date_string]['MIN']))
            example.append_temp_max(float(records[date_string]['MAX']))
            example.append_temp_avg(float(records[date_string]['AVG']))

        return example

    def bounding_box(self, lat, lng, max_station_distance):
        from geopy import Point, distance
        location = Point(lat, lng)
        sw = distance.VincentyDistance(miles=max_station_distance).destination(location, 225)
        ne = distance.VincentyDistance(miles=max_station_distance).destination(location, 45)
        return (sw, ne)

RADIANT_TO_KM_CONSTANT = 6367
class BallTreeIndex:
    def __init__(self,lat_longs):
        from sklearn.neighbors import BallTree
        import numpy as np
        self.lat_longs = lat_longs
        self.ball_tree_index = BallTree(np.radians(lat_longs), metric='haversine')

    def query_radius(self,lat, lng ,radius_miles):
        import numpy as np
        import geopy as geopy
        from geopy.distance import vincenty
        # radius_km = radius/1e3
        radius_km = radius_miles * 0.6
        radius_radian = radius_km / RADIANT_TO_KM_CONSTANT
        query = np.radians(np.array([(lat, lng)]))
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

class _WeatherLoader():
    def __init__(self, project, bbox, year, month, weather_station_distance, days_before):
        self._project = project
        self._dataset = 'bigquery-public-data:noaa_gsod'
        self._weather_station_distance = weather_station_distance
        self._bbox = bbox # swLng, swLat, neLng, neLat
        self._year = int(year)
        self._month = int(month)
        self._map = {}
        self._days_before = days_before
        self._load()

    # year_dictionary provides {2016: ["01"], 2015: [12]}
    def _year_dictionary(self, range):
        res = {}
        for d in range.tolist():
            if str(d.year) not in res:
                res[str(d.year)] = set()
            res[str(d.year)].add('{:02d}'.format(d.month))
        return res

    def _form_query(self, q_year, q_months, bbox):
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
                     b.lat <= {q_n_lat} AND b.lat >= {q_s_lat}
                    AND b.lon >= {q_w_lon} AND b.lon <= {q_e_lon}
                    AND {q_month}
                  ORDER BY
                    a.da DESC
            """
        values = {
            'q_n_lat': str(bbox[3]),
            'q_w_lon': str(bbox[0]),
            'q_s_lat': str(bbox[1]),
            'q_e_lon': str(bbox[2]),
            'q_year': q_year,
            'q_month': month_query
        }

        return q.format(**values)

    def _load(self):
        from datetime import datetime
        from pandas import date_range
        from google.cloud import bigquery
        from calendar import monthrange

        _client = bigquery.Client(project=self._project)

        years = self._year_dictionary(date_range(
            end=datetime(self._year, self._month, monthrange(self._year, self._month)[1]),
            periods=5,
            freq='M'
        ))

        station_placeholder = {}
        stations = []

        for year, months in years.iteritems():
            query = _client.query(self._form_query(year, months, self._bbox))

            # Had some trouble with conflicting versions of this package between local runner and remote.
            #  pip install --upgrade google-cloud-bigquery
            # sync_query = _client.run_sync_query(query)
            # sync_query.timeout_ms = 30000
            # sync_query.run()

            for row in query.result(timeout=30000):

            # page_token=None
            # while True:
            #     iterator = sync_query.fetch_data(
            #         page_token=page_token
            #     )
            #     for row in iterator:
                    k = '%.6f-%.6f' % (row[0], row[1])
                    if k not in station_placeholder:
                        station_placeholder[k] = 1
                        stations.append((row[0], row[1]))
                        self._map[k] = {}

                    # station_path = self._temp_directory+"/"+k+"/"
                    #
                    # try:
                    #     os.makedirs(station_path)
                    # except OSError:
                    #     if not os.path.isdir(station_path):
                    #         raise

                    self._map[k][row[8]+row[6]+row[7]] = {
                        'PRCP': row[2],
                        'MIN': row[3],
                        'MAX': row[4],
                        'AVG': row[5]
                    }
                    # with open(station_path+row[8]+row[6]+row[7]+".csv", "w") as f:
                        # prcp, min, max, temp
                        # f.write('%.6f, %.6f, %.6f, %.6f' % (row[2], row[3], row[4], row[5]))
                # if iterator.next_page_token is None:
                #     break
                # page_token = iterator.next_page_token

        self._stations_index = BallTreeIndex(stations)

    def _read_one(self, lat_lng_keys, date):
        # import os
        d = date.strftime("%Y%m%d")

        for k in lat_lng_keys:
            if k in self._map:
                if d in self._map[k]:
                    return self._map[k][d]

            # filepath = ('%s/%s/%s.csv' % (self._temp_directory, k, date.strftime("%Y%m%d")))
            #
            # if os.path.exists(filepath) is False:
            #     continue
            #
            # with open(filepath, 'r') as file:
            #     vs = file.read().replace('\n', '').split(',')
            #
            #     return {
            #         'PRCP': vs[0],
            #         'MIN': vs[1],
            #         'MAX': vs[2],
            #         'AVG': vs[3]
            #     }

        return {}

    def read(self, lat, lng, today):
        import pandas as pd
        from datetime import timedelta

        records = {}

        station_list = self._stations_index.query_radius(lat, lng, self._weather_station_distance)

        for d in pd.date_range(
            # Set one day in the past to ensure we have all data.
            # Should only be relevant to when fetching the daily batch.
            end = today - timedelta(days=1),
            periods=self._days_before,
            freq='D'):

            o = self._read_one(station_list, d)
            if not o:
                return {}
            records[d.strftime("%Y%m%d")] = o

        return records
