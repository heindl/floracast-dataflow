from tensorflow.core.example import example_pb2
import apache_beam as beam

@beam.typehints.with_input_types(example_pb2.SequenceExample)
@beam.typehints.with_output_types(example_pb2.SequenceExample)
class FetchWeatherDoFn(beam.DoFn):
    def __init__(self, project, weather_station_distance):
        super(FetchWeatherDoFn, self).__init__()
        from apache_beam.metrics import Metrics
        self._project = project
        self._dataset = 'bigquery-public-data:noaa_gsod'
        self.insufficient_weather_records = Metrics.counter('main', 'insufficient_weather_records')
        self._weather_station_distance = weather_station_distance

    def process(self, example):
        from geopy import Point, distance
        from google.cloud import bigquery
        import astral
        import logging
        from pandas import date_range
        from datetime import datetime

        client = bigquery.Client(project=self._project)

        lat = example.context.feature['latitude'].float_list.value[0]
        lng = example.context.feature['longitude'].float_list.value[0]

        location = Point(lat, lng)

        # Calculate bounding box.
        nw = distance.VincentyDistance(miles=self._weather_station_distance).destination(location, 315)
        se = distance.VincentyDistance(miles=self._weather_station_distance).destination(location, 135)

        records = {}

        yearmonths = {}
        date = datetime.fromtimestamp(example.context.feature['date'].int64_list.value[0])
        range = date_range(end=datetime(date.year, date.month, date.day), periods=45, freq='D')
        for d in range.tolist():
            if str(d.year) not in yearmonths:
                yearmonths[str(d.year)] = set()
            yearmonths[str(d.year)].add('{:02d}'.format(d.month))

        for year, months in yearmonths.iteritems():
            month_query = ""
            for m in months:
                if month_query == "":
                    month_query = "(mo = '%s'" % m
                else:
                    month_query += " OR mo = '%s'" % m
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
                    [bigquery-public-data:noaa_gsod.gsod{year}] a
                  JOIN
                    [bigquery-public-data:noaa_gsod.stations] b
                  ON
                    a.stn=b.usaf
                    AND a.wban=b.wban
                  WHERE
                     lat <= {nLat} AND lat >= {sLat}
                    AND lon >= {wLon} AND lon <= {eLon}
                    AND {monthQuery}
                  ORDER BY
                    da DESC
            """
            values = {
                'nLat': str(nw.latitude),
                'wLon': str(nw.longitude),
                'sLat': str(se.latitude),
                'eLon': str(se.longitude),
                'year': year,
                'monthQuery': month_query
            }

            # Had some trouble with conflicting versions of this package between local runner and remote.
            #  pip install --upgrade google-cloud-bigquery
            sync_query = client.run_sync_query(q.format(**values))
            sync_query.timeout_ms = 30000
            sync_query.run()

            page_token=None
            while True:
                iterator = sync_query.fetch_data(
                    max_results=1000,
                    page_token=page_token
                )
                for row in iterator:

                    d = datetime(int(row[8]), int(row[6]), int(row[7]))
                    if d > range.max() or d < range.min():
                        continue
                    if d in records:
                        previous = distance.vincenty().measure(Point(records[d][0], records[d][1]), location)
                        current = distance.vincenty().measure(Point(row[0], row[1]), location)
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
            self.insufficient_weather_records.inc()
            return

        tmax = example.feature_lists.feature_list["tmax"]
        tmin = example.feature_lists.feature_list["tmin"]
        prcp = example.feature_lists.feature_list["prcp"]
        temp = example.feature_lists.feature_list["temp"]
        daylight = example.feature_lists.feature_list["daylight"]

        for d in dates:

            daylength = 0
            try:
                a = astral.Astral()
                a.solar_depression = 'civil'
                astro = a.sun_utc(d, lat, lng)
                daylength = (astro['sunset'] - astro['sunrise']).seconds
            except astral.AstralError as err:
                if "Sun never reaches 6 degrees below the horizon" in err.message:
                    daylength = 86400
                else:
                    logging.error("Error parsing day[%s] length at [%.6f,%.6f]: %s", date, lat, lng, err)
                    return

            daylight.feature.add().float_list.value.append(daylength)
            prcp.feature.add().float_list.value.append(records[d][2])
            tmin.feature.add().float_list.value.append(records[d][3])
            tmax.feature.add().float_list.value.append(records[d][4])
            temp.feature.add().float_list.value.append(records[d][5])

        yield example