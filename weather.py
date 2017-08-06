from __future__ import absolute_import

import apache_beam as beam
from datetime import datetime, timedelta
from google.cloud import bigquery
from geopy import Point, distance
import occurrence as fo
import pandas as pd
from apache_beam.metrics import Metrics
import tensorflow as tf
import logging

import dateutil.parser
# If fails:
# gcloud auth application-default login
@beam.typehints.with_input_types(tf.train.SequenceExample)
@beam.typehints.with_output_types(tf.train.SequenceExample)
class FetchWeatherDoFn(beam.DoFn):
    def __init__(self, project):
        super(FetchWeatherDoFn, self).__init__()
        self._project = project
        self._dataset = 'bigquery-public-data:noaa_gsod'
        self.insufficient_weather_records = Metrics.counter('main', 'insufficient_weather_records')

    def process(self, example):
        client = bigquery.Client(project=self._project)

        lat = example.context.feature['latitude'].float_list.value[0]
        lng = example.context.feature['longitude'].float_list.value[0]

        location = Point(lat, lng)

        # Calculate bounding box.
        nw = distance.VincentyDistance(miles=40).destination(location, 315)
        se = distance.VincentyDistance(miles=40).destination(location, 135)

        records = {}

        yearmonths = {}
        date = datetime.fromtimestamp(example.context.feature['date'].int64_list.value[0])
        range = pd.date_range(end=date, periods=45, freq='D')
        for d in range.tolist():
            if str(d.year) not in yearmonths:
                yearmonths[str(d.year)] = set()
            yearmonths[str(d.year)].add('{:02d}'.format(d.month))

        for year, months in yearmonths.iteritems():
            monthquery = ""
            for m in months:
                if monthquery == "":
                    monthquery = "(mo = '%s'" % m
                else:
                    monthquery += " OR mo = '%s'" % m
            monthquery += ")"

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
                    year
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
                'monthQuery': monthquery
            }

            query_results = client.run_sync_query(q.format(**values))
            query_results.run()
            rows = query_results.fetch_data(max_results=1000)

            for row in rows:
                d = datetime(int(row[8]), int(row[6]), int(row[7]))
                if d > range.max() or d < range.min():
                    continue
                if d in records:

                    previous = distance.vincenty().measure(Point(records[d][0], records[d][1]), location)
                    current = distance.vincenty().measure(Point(row[0], row[1]), location)
                    if current < previous:
                        records[d] = row
                        continue
                records[d] = row

        dates = records.keys()
        dates.sort()


        if len(dates) != (len(range) - 1):
            print(len(dates), len(range))
            # logging.info("range: %.8f, %.8f, %s, %s", lat, lng, year, months)
            self.insufficient_weather_records.inc()
            return

        tmax = example.feature_lists.feature_list["tmax"]
        tmin = example.feature_lists.feature_list["tmin"]
        prcp = example.feature_lists.feature_list["prcp"]
        temp = example.feature_lists.feature_list["temp"]

        for d in dates:
            tmax.feature.add().float_list.value.append(records[d][2])
            tmin.feature.add().float_list.value.append(records[d][3])
            prcp.feature.add().float_list.value.append(records[d][4])
            temp.feature.add().float_list.value.append(records[d][5])

        yield example