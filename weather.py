from __future__ import absolute_import

import apache_beam as beam
from datetime import datetime, timedelta
from google.cloud import bigquery
from geopy import Point
from geopy.distance import VincentyDistance

import dateutil.parser

@beam.typehints.with_input_types(Occurrence)
@beam.typehints.with_output_types(Occurrence)
class FetchWeatherDoFn(beam.DoFn):
    def __init__(self, project):
        super(FetchWeatherDoFn, self).__init__()
        self._project = project
        self._dataset = 'bigquery-public-data:noaa_gsod'

    def process(self, o):
        client = bigquery.Client(project=self._project)

        o.latitude,

        # Calculate bounding box.
        nwlat, nwlon = VincentyDistance(miles=20).destination(Point(o.latitude, o.longitude), 315)
        swlat, swlon = VincentyDistance(miles=20).destination(Point(o.latitude, element.longitude), 135)

        # Calculate date.
        e = dateutil.parser.parse(element.date)
        b = e - datetime.timedelta(days=30)

        query_results = client.run_sync_query("""
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
                [bigquery-public-data:noaa_gsod.gsod2015] a
              JOIN
                [bigquery-public-data:noaa_gsod.stations] b
              ON
                a.stn=b.usaf
                AND a.wban=b.wban
              WHERE
                lat < %(nw-lat) AND lat > %(se-lat) 
                AND lon > %(nw-lon) AND lon < %(se-lon)
                AND (YEAR == %(b-year) OR YEAR == %(e-year))
                AND (MONTH == %(b-month) OR MONTH == %(e-month))
              ORDER BY
                max DESC
        """ % {
            'nw-lat': nwlat,
            'nw-lon': nwlon,
            'sw-lat': swlat,
            'sw-lon': swlon,
            'b-year': b.year,
            'b-month': b.month,
            'e-year': e.year,
            'e-month': e.month,
        })

        query_results.run()
        rows = query_results.fetch_data(max_results=100)

        for row in rows:
            print(row)