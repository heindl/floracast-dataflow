# from __future__ import absolute_import
import apache_beam as beam
from example import Example


@beam.typehints.with_input_types(Example)
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

    def process(self, example):
        import astral
        import logging
        from datetime import datetime

        records = {}

        for max_station_distance in [20, 40, 60]:
            records = self.fetch(example.latitude(), example.longitude(), datetime.fromtimestamp(example.date()), max_station_distance)
            if len(records) == 0:
                continue
            else:
                break

        if len(records) == 0:
            self._insufficient_weather_records.inc()
            return

        for date in records:

            daylength = 0
            try:
                a = astral.Astral()
                a.solar_depression = 'civil'
                astro = a.sun_utc(date, example.latitude(), example.longitude())
                daylength = (astro['sunset'] - astro['sunrise']).seconds
            except astral.AstralError as err:
                if "Sun never reaches 6 degrees below the horizon" in err.message:
                    daylength = 86400
                else:
                    logging.error("Error parsing day[%s] length at [%.6f,%.6f]: %s", date, example.latitude(), example.longitude(), err)
                    return

            example.append_daylight(daylength)
            example.append_precipitation(records[date][2])
            example.append_temp_min(records[date][3])
            example.append_temp_max(records[date][4])
            example.append_temp_avg(records[date][5])

        self._sufficient_weather_records.inc()

        yield example

    # year_dictionary provides {2016: ["01"], 2015: [12]}
    def year_dictionary(self, range):
        res = {}
        for d in range.tolist():
            if str(d.year) not in res:
                res[str(d.year)] = set()
            res[str(d.year)].add('{:02d}'.format(d.month))
        return res

    def bounding_box(self, lat, lng, max_station_distance):
        from geopy import Point, distance
        location = Point(lat, lng)
        nw = distance.VincentyDistance(miles=max_station_distance).destination(location, 315)
        se = distance.VincentyDistance(miles=max_station_distance).destination(location, 135)
        return (nw, se)

    def form_query(self, q_year, q_months, nw_point, se_point):
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
            'q_n_lat': str(nw_point.latitude),
            'q_w_lon': str(nw_point.longitude),
            'q_s_lat': str(se_point.latitude),
            'q_e_lon': str(se_point.longitude),
            'q_year': q_year,
            'q_month': month_query
        }

        return q.format(**values)

    def fetch(self, lat, lng, date, max_station_distance):
        from datetime import datetime, timedelta
        from pandas import date_range
        from geopy import Point, distance
        from google.cloud import bigquery

        occurrence_location = Point(lat, lng)
        nw, se = self.bounding_box(lat, lng, max_station_distance)

        _client = bigquery.Client(project=self._project)

        # It appears that most weather updates are a few days behind. In leu of using forecast values
        # from another datasource for now, let's use a few days behind the date for all values.
        today = datetime(date.year, date.month, date.day)
        end = today - timedelta(days=1)
        range = date_range(end=end, periods=45, freq='D')
        records = {}
        for year, months in self.year_dictionary(range).iteritems():

            query = self.form_query(year, months, nw, se)

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

                    d = datetime(int(row[8]), int(row[6]), int(row[7]))
                    if d > range.max() or d < range.min():
                        continue
                    if d in records:
                        previous = distance.vincenty().measure(Point(records[d][0], records[d][1]), occurrence_location)
                        current = distance.vincenty().measure(Point(row[0], row[1]), occurrence_location)
                        if current < previous:
                            records[d] = row
                    else:
                        records[d] = row
                if iterator.next_page_token is None:
                    break
                page_token = iterator.next_page_token

        dates = records.keys()
        dates.sort()
        if len(dates) != len(range):
            # logging.info("range: %.8f, %.8f, %s, %s", lat, lng, year, months)
            return {}
        else:
            return records

