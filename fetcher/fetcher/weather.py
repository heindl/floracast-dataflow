# from __future__ import absolute_import
import apache_beam as beam
from example import Example

@beam.typehints.with_input_types(beam.typehints.Tuple[str, beam.typehints.Iterable[Example]])
@beam.typehints.with_output_types(Example)
class FetchWeatherDoFn(beam.DoFn):
    def __init__(self, project, weather_station_distance):
        super(FetchWeatherDoFn, self).__init__()
        from apache_beam.metrics import Metrics
        self._project = project
        self._dataset = 'bigquery-public-data:noaa_gsod'
        self._insufficient_weather_records = Metrics.counter('main', 'insufficient_weather_records')
        self._sufficient_weather_records = Metrics.counter('main', 'sufficient_weather_records')
        self._weather_station_distance = weather_station_distance

    def process(self, batch):
        import astral
        import logging
        from datetime import datetime

        # First identify a bounding box.
        # [33.2398, -85.0795], [34.1448, -83.3327]
        sw, ne = [0, 0], [0, 0]

        for example in batch[1]:
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

        print("bounding box", sw.longitude, sw.latitude, ne.longitude, ne.latitude)
        print("year/month", batch[0][0:4], batch[0][4:6])

        store = _WeatherLoader(
            project=self._project,
            bbox=[sw.longitude, sw.latitude, ne.longitude, ne.latitude],
            year=batch[0][0:4],
            month=batch[0][4:6],
            weather_station_distance=self._weather_station_distance)

        for example in batch[1]:

            records = store.read(example.latitude(), example.longitude(), datetime.fromtimestamp(example.date()))

            print("records", example.latitude(), example.longitude(), datetime.fromtimestamp(example.date()), len(records.keys()))

            if len(records.keys()) == 0:
                self._insufficient_weather_records.inc()
                continue

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
                        return

                example.append_daylight(day_length)
                example.append_precipitation(float(records[date_string]['PRCP']))
                example.append_temp_min(float(records[date_string]['MIN']))
                example.append_temp_max(float(records[date_string]['MAX']))
                example.append_temp_avg(float(records[date_string]['AVG']))

            self._sufficient_weather_records.inc()

            yield example

    def bounding_box(self, lat, lng, max_station_distance):
        from geopy import Point, distance
        location = Point(lat, lng)
        sw = distance.VincentyDistance(miles=max_station_distance).destination(location, 225)
        ne = distance.VincentyDistance(miles=max_station_distance).destination(location, 45)
        return (sw, ne)


class _WeatherLoader(beam.DoFn):
    def __init__(self, project, bbox, year, month, weather_station_distance):
        import tempfile
        super(_WeatherLoader, self).__init__()
        self._project = project
        self._dataset = 'bigquery-public-data:noaa_gsod'
        self._weather_station_distance = weather_station_distance
        self._bbox = bbox # swLng, swLat, neLng, neLat
        self._year = int(year)
        self._month = int(month)
        self._temp_directory = tempfile.mkdtemp()
        print("temp_directory", self._temp_directory)
        self._load()

    # year_dictionary provides {2016: ["01"], 2015: [12]}
    def _year_dictionary(self, range):
        res = {}
        for d in range.tolist():
            if str(d.year) not in res:
                res[str(d.year)] = set()
            res[str(d.year)].add('{:02d}'.format(d.month))
        return res

    def _polygon(self, lat, lng):
        from geopy import Point, distance
        location = Point(lat, lng)
        sw = distance.VincentyDistance(miles=self._weather_station_distance).destination(location, 225)
        ne = distance.VincentyDistance(miles=self._weather_station_distance).destination(location, 45)
        return [
            [sw.longitude, sw.latitude],
            [sw.longitude, ne.latitude],
            [ne.longitude, ne.latitude],
            [ne.longitude, sw.latitude]
        ]

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
                    elev
                  FROM
                    [bigquery-public-data:noaa_gsod.gsod{q_year}] a
                  JOIN
                    [bigquery-public-data:noaa_gsod.stations] b
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
        from datetime import datetime, timedelta
        from pandas import date_range
        from google.cloud import bigquery
        from calendar import monthrange
        _client = bigquery.Client(project=self._project)

        years = self._year_dictionary(date_range(
            end=datetime(self._year, self._month, monthrange(self._year, self._month)[1]),
            periods=5,
            freq='M'
        ))

        for year, months in years.iteritems():
            query = self._form_query(year, months, self._bbox)

            # Had some trouble with conflicting versions of this package between local runner and remote.
            #  pip install --upgrade google-cloud-bigquery
            sync_query = _client.run_sync_query(query)
            sync_query.timeout_ms = 30000
            sync_query.run()

            page_token=None
            while True:
                iterator = sync_query.fetch_data(
                    page_token=page_token
                )
                for row in iterator:
                    with open(self._temp_directory+"/"+row[8]+row[6]+row[7]+".csv", "a") as f:
                        # location, prcp, min, max, temp
                        f.write('%.6f, %.6f, %.6f, %.6f, %.6f, %.6f\n' % (row[0], row[1], row[2], row[3], row[4], row[5]))
                if iterator.next_page_token is None:
                    break
                page_token = iterator.next_page_token

    def read(self, lat, lng, today):
        import pandas as pd
        # import geopandas as gpd
        # from shapely import geometry
        from datetime import timedelta
        import geopy as geopy
        from geopy.distance import vincenty

        records = {}
        polygon = self._polygon(lat, lng)

        for d in pd.date_range(
            # Set one day in the past to ensure we have all data.
            # Should only be relevant to when fetching the daily batch.
            end = today - timedelta(days=1),
            periods=90,
            freq='D'
        ):
            weather = pd.read_csv('%s/%s.csv' % (self._temp_directory, d.strftime("%Y%m%d")),
                header=None,
                names=['Y','X','PRCP','MIN','MAX','AVG']
            )
            # csv['geometry'] = csv.apply(lambda z: geometry.Point(float(z.X), float(z.Y)), axis=1)
            # Initial filter in order to avoid massive sort.
            # weather = weather[weather.apply(lambda z: geometry.Point(float(z.X), float(z.Y)).within(geometry.Polygon(polygon)), axis=1)]
            weather['Distance'] = weather.apply(lambda z: vincenty(geopy.Point(lat, lng), geopy.Point(float(z.Y), float(z.X))).miles, axis=1)
            weather = weather[weather.Distance <= self._weather_station_distance]
            weather = weather.sort_values(['Distance'], ascending=[1])
            for index, row in weather.iterrows():
                if row['Distance'] > self._weather_station_distance:
                    print("date, distance/row", d, self._weather_station_distance, row['Distance'])
                    return {} # Short circuit because we're missing a day and no need to continue
                records[d.strftime('%Y%m%d')] = row
                break

        return records
