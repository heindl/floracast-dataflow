from __future__ import absolute_import

import argparse
import logging

import googledatastore as ds
from apache_beam.io.gcp.datastore.v1.datastoreio import ReadFromDatastore
from apache_beam.metrics.metric import MetricsFilter
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from google.cloud.proto.datastore.v1 import query_pb2
from googledatastore import helper as datastore_helper, PropertyFilter
import random
import mgrs
import os
from googlemaps import Client
import apache_beam as beam
from datetime import datetime, timedelta
from google.cloud import bigquery
from geopy import Point, distance
import pandas as pd
from apache_beam.metrics import Metrics
import tensorflow as tf
import logging
import astral
import dateutil.parser

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()

@beam.typehints.with_input_types(beam.typehints.KV[int, beam.typehints.Iterable[tf.train.SequenceExample]])
@beam.typehints.with_output_types(tf.train.SequenceExample)
class UnwindTaxaWithSufficientCount(beam.DoFn):
    def __init__(self):
        super(UnwindTaxaWithSufficientCount, self).__init__()
        self.sufficient_taxa_counter = Metrics.counter('main', 'sufficient_taxa')
        self.insufficient_taxa_counter = Metrics.counter('main', 'insufficient_taxa')

    def process(self, element):
        print("length")
        print(len(element[1]))
        if len(element[1]) <= 50:
            self.insufficient_taxa_counter.inc()
            return

        self.sufficient_taxa_counter.inc()

        locations = []
        blanks = []
        for o in element[1]:
            yield o

            lat, lng, date = rand.random_point()
            locations.append((lat, lng))
            se = tf.train.SequenceExample()
            se.context.feature["label"].int64_list.value.append(0)
            se.context.feature["latitude"].float_list.value.append(lat)
            se.context.feature["longitude"].float_list.value.append(lng)
            se.context.feature["date"].int64_list.value.append(int(date.strftime("%s")))
            se.context.feature["grid-zone"].bytes_list.value.append(mgrs.MGRS().toMGRS(lat, lng)[:2])
            blanks.append(se)

        client = Client(key=os.environ["FLORACAST_GOOGLE_MAPS_API_KEY"])

        # Group here in order to run wider batches
        for batch in group_list(locations, 300):
            for r in client.elevation(batch):
                for i, b in enumerate(blanks):
                    if r['location']['lat'] != b.context.feature['latitude'].float_list.value[0]:
                        continue
                    if r['location']['lng'] != b.context.feature['longitude'].float_list.value[0]:
                        continue
                    blanks[i].context.feature["elevation"].float_list.value.append(b['elevation'])
                    break

        for b in blanks:
            yield b


def group_list(l, group_size):
    """
    :param l:           list
    :param group_size:  size of each group
    :return:            Yields successive group-sized lists from l.
    """
    for i in xrange(0, len(l), group_size):
        yield l[i:i+group_size]


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--output',
                        dest='output',
                        # CHANGE 1/5: The Google Cloud Storage path is required
                        # for outputting the results.
                        default='./records',
                        help='Output file to write results to.')
    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_args.extend([
        # CHANGE 2/5: (OPTIONAL) Change this to DataflowRunner to
        # run your pipeline on the Google Cloud Dataflow Service.
        '--runner=DirectRunner',
        # CHANGE 3/5: Your project ID is required in order to run your pipeline on
        # the Google Cloud Dataflow Service.
        '--project=floracast-20c01',
        # CHANGE 4/5: Your Google Cloud Storage path is required for staging local
        # files.
        '--staging_location=gs://YOUR_BUCKET_NAME/AND_STAGING_DIRECTORY',
        # CHANGE 5/5: Your Google Cloud Storage path is required for temporary
        # files.
        '--temp_location=gs://YOUR_BUCKET_NAME/AND_TEMP_DIRECTORY',
        '--job_name=occurrence-fetcher',
    ])

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True

    p = beam.Pipeline(options=pipeline_options)

    project = pipeline_options.get_all_options()['project']


    # q = query.Query(kind='Occurrence', project=project)
    # q.fetch()
    q = query_pb2.Query()
    datastore_helper.set_kind(q, 'Occurrence')
    # q.limit.name = 100

    # Need to index this in google.

    # datastore_helper.set_composite_filter(q.filter, CompositeFilter.AND,
        # datastore_helper.set_property_filter(ds.Filter(), 'Location.Lat', PropertyFilter.GREATER_THAN_OR_EQUAL, 5.4995),
        # datastore_helper.set_property_filter(ds.Filter(), 'Location.Lat', PropertyFilter.LESS_THAN_OR_EQUAL, 83.1621),
        # datastore_helper.set_property_filter(ds.Filter(), 'Location.Lng', PropertyFilter.GREATER_THAN_OR_EQUAL, -167.2764),
    # https://stackoverflow.com/questions/41705870/geospatial-query-at-google-app-engine-datastore
    # The query index may not be implemented at this point.
    datastore_helper.set_property_filter(ds.Filter(), 'Location.longitude', PropertyFilter.LESS_THAN_OR_EQUAL, -52.2330)
    # )

    lines = p \
        | 'ReadDatastoreOccurrences' >> ReadFromDatastore(project=project, query=q, num_splits=0) \
        | 'ConvertEntitiesToKeyStrings' >> beam.ParDo(foccurrence.EntityToString()) \
        | 'RemoveDuplicates' >> beam.RemoveDuplicates() \
        | 'ConvertKeyStringsToTaxonSequenceExample' >> beam.ParDo(foccurrence.StringToTaxonSequenceExample()) \
        | 'GroupByTaxon' >> beam.GroupByKey() \
        | 'UnwindSufficientTaxa' >> beam.ParDo(UnwindTaxaWithSufficientCount()) \
        | 'AttachWeather' >> beam.ParDo(fweather.FetchWeatherDoFn(project))

    lines | 'Write' >> beam.io.WriteToTFRecord(known_args.output)

    result = p.run()

    result.wait_until_finish()

    for v in [
        'new_occurrences',
        'invalid_occurrence_elevation',
        'invalid_occurrence_location',
        'invalid_occurrence_date',
        'sufficient_taxa',
        'insufficient_taxa',
        'insufficient_weather_records',
        'final_occurrence_count',
    ]:
        query_result = result.metrics().query(MetricsFilter().with_name(v))
        if query_result['counters']:
            logging.info('%s: %d', v, query_result['counters'][0].committed)



class SequenceExampleCoder(beam.coders.Coder):

    def encode(self, o):
        return o.SerializeToString()

    def decode(self, str):
        se = tf.train.SequenceExample()
        se.ParseFromString(str)

        #
        #
        # se = tf.train.SequenceExample()
        # for k, v in context_parsed:
        #
        # se.context = tf.train.Features(feature=context_parsed)
        # se.feature_lists = tf.train.FeatureLists(feature_list=sequence_parsed)
        return se

    def is_deterministic(self):
        return True


beam.coders.registry.register_coder(tf.train.SequenceExample, SequenceExampleCoder)

# Filter and prepare for duplicate sort.
@beam.typehints.with_input_types(datastore.Entity)
@beam.typehints.with_output_types(str)
class EntityToString(beam.DoFn):
    def __init__(self):
        super(EntityToString, self).__init__()
        self.new_occurrence_counter = Metrics.counter('main', 'new_occurrences')
        self.invalid_occurrence_date = Metrics.counter('main', 'invalid_occurrence_date')
        self.invalid_occurrence_elevation = Metrics.counter('main', 'invalid_occurrence_elevation')
        self.invalid_occurrence_location = Metrics.counter('main', 'invalid_occurrence_location')

    def process(self, element):
        """
            Element should be an occurrence entity.
            The key has should be a sufficient key.
        """
        e = helpers.entity_from_protobuf(element)

        if e.key.parent.parent.id != 60606:
            return

        self.new_occurrence_counter.inc()

        # This is a hack to avoid indexing the 'Date' property in Go.
        if e['Date'].year < 1970:
            self.invalid_occurrence_date.inc()
            return

        if 'Elevation' not in e:
            self.invalid_occurrence_elevation.inc()
            return

        loc = e['Location']
        elevation = e['Elevation']

        (lat, lng) = (0.0, 0.0)
        if type(loc) is helpers.GeoPoint:
            lat = loc.latitude
            lng = loc.longitude
        elif type(loc) is datastore.entity.Entity:
            lat = loc['Lat']
            lng = loc['Lng']
        else:
            logging.error("invalid type: %s", type(loc))
            return

        if lng > -52.2330:
            logging.info("%.6f && %.6f", lat, lng)
            self.invalid_occurrence_location.inc()
            return

        yield "%d|||%.8f|||%.8f|||%d|||%.8f" % (
            e.key.parent.parent.id,
            lat,
            lng,
            int(e['Date'].strftime("%s")),
            elevation
        )

@beam.typehints.with_input_types(str)
@beam.typehints.with_output_types(beam.typehints.KV[int, tf.train.SequenceExample])
class StringToTaxonSequenceExample(beam.DoFn):
    def __init__(self):
        super(StringToTaxonSequenceExample, self).__init__()

    def process(self, element):

        ls = element.split("|||")
        taxon = int(ls[0])
        lat = float(ls[1])
        lng = float(ls[2])
        intDate = int(ls[3])
        elevation = float(ls[4])

        se = tf.train.SequenceExample()
        se.context.feature["label"].int64_list.value.append(taxon)
        se.context.feature["latitude"].float_list.value.append(lat)
        se.context.feature["longitude"].float_list.value.append(lng)
        se.context.feature["date"].int64_list.value.append(intDate)
        se.context.feature["elevation"].float_list.value.append(elevation)
        se.context.feature["grid-zone"].bytes_list.value.append(mgrs.MGRS().toMGRS(lat, lng)[:2])

        yield taxon, se

@beam.typehints.with_input_types(tf.train.SequenceExample)
@beam.typehints.with_output_types(tf.train.SequenceExample)
class FetchWeatherDoFn(beam.DoFn):
    def __init__(self, project):
        super(FetchWeatherDoFn, self).__init__()
        self._project = project
        self._dataset = 'bigquery-public-data:noaa_gsod'
        self.insufficient_weather_records = Metrics.counter('main', 'insufficient_weather_records')
        self.final_occurrence_count = Metrics.counter('main', 'final_occurrence_count')

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
        range = pd.date_range(end=date, periods=60, freq='D')
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
            tmax.feature.add().float_list.value.append(records[d][2])
            tmin.feature.add().float_list.value.append(records[d][3])
            prcp.feature.add().float_list.value.append(records[d][4])
            temp.feature.add().float_list.value.append(records[d][5])

        self.final_occurrence_count.inc()

        yield example


NORTHERNMOST = 49.
SOUTHERNMOST = 25.
EASTERNMOST = -66.
WESTERNMOST = -124.

def random_point():

    date = datetime(
        random.randint(2015, 2017),
        random.randint(1, 12),
        random.randint(1, 28)
    )
    while True:
        lat = round(random.uniform(SOUTHERNMOST, NORTHERNMOST), 6)
        lng = round(random.uniform(EASTERNMOST, WESTERNMOST), 6)
        try:
            gcode = Geocoder.reverse_geocode(lat, lng)

            if gcode[0].data[0]['formatted_address'][-6:] in ('Canada', 'Mexico'):
                continue
            elif 'unnamed road' in gcode[0].data[0]['formatted_address']:
                continue
            elif 'Unnamed Road' in gcode[0].data[0]['formatted_address']:
                continue
            else:
                return gcode[0].coordinates[0], gcode[0].coordinates[1], date
        except GeocoderError:
            continue
