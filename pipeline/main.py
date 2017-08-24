from __future__ import absolute_import
from apache_beam.options.pipeline_options import PipelineOptions
import logging
import tensorflow as tf
from google.cloud.proto.datastore.v1 import entity_pb2
import apache_beam as beam
# pip install "apache_beam[gcp]"

class OccurrencePipelineOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):

        parser.add_argument(
            '--output',
            required=True,
            help='Output file to write results to.')

        # parser.add_argument(
        #     '--streaming',
        #     required=False,
        #     default=False,
        #     help='Whether streaming mode is enabled or disabled; true if enabled.')

        # parser.add_argument(
        #     '--runner',
        #     required=False,
        #     default="DataflowRunner",
        #     help='DataflowRunner or DirectRunner')

        # parser.add_argument(
        #     '--project',
        #     required=True,
        #     help='Your project ID is required in order to run your pipeline on the Google Cloud Dataflow Service.')

        # parser.add_argument(
        #     '--template_location',
        #     required=False,
        #     help='Location of template file')

        # parser.add_argument(
        #     '--staging_location',
        #     required=False,
        #     default="gs://occurrence_dataflow/staging/",
        #     help='Your Google Cloud Storage path is required for staging local files.')

        # parser.add_argument(
        #     '--temp_location',
        #     required=False,
        #     default="gs://occurrence_dataflow/temp/",
        #     help='Your Google Cloud Storage path is required for temporary files.')

        # parser.add_argument(
        #     '--job_name',
        #     required=True,
        #     help='Name of the dataflow job.')

        parser.add_argument(
            '--minimum_occurrences_within_taxon',
            required=False,
            default=40,
            help='The number of occurrence required to process taxon')

        parser.add_argument(
            '--taxon',
            required=False,
            default=60606,
            help='Restrict occurrence fetch to this taxa')

        parser.add_argument(
            '--weather_station_distance',
            required=False,
            default=100,
            help='Maximum distance a weather station can be from an occurrence when fetching weather.')


# Filter and prepare for duplicate sort.
@beam.typehints.with_input_types(entity_pb2.Entity)
@beam.typehints.with_output_types(str)
class EntityToString(beam.DoFn):

    def __init__(self, taxon=0):
        from apache_beam.metrics import Metrics
        super(EntityToString, self).__init__()
        self.new_occurrence_counter = Metrics.counter('main', 'new_occurrences')
        self.invalid_occurrence_date = Metrics.counter('main', 'invalid_occurrence_date')
        self.invalid_occurrence_elevation = Metrics.counter('main', 'invalid_occurrence_elevation')
        self.invalid_occurrence_location = Metrics.counter('main', 'invalid_occurrence_location')
        self._taxon = taxon

    def process(self, element):
        from google.cloud.datastore.helpers import entity_from_protobuf, GeoPoint
        from google.cloud.datastore import Entity
        import logging
        """
            Element should be an occurrence entity.
            The key has should be a sufficient key.
        """
        e = entity_from_protobuf(element)

        if self._taxon != 0:
            if e.key.parent.parent.id != self._taxon:
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
        if type(loc) is GeoPoint:
            lat = loc.latitude
            lng = loc.longitude
        elif type(loc) is Entity:
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

# Unwind taxa with a sufficient number of occurrences, and add a random test point to each occurrence.
# It's confusing to include the random point creation here, but it allows us to batch elevation queries
# and save on api hits to google maps.
@beam.typehints.with_input_types(beam.typehints.KV[int, beam.typehints.Iterable[tf.train.SequenceExample]])
@beam.typehints.with_output_types(tf.train.SequenceExample)
class UnwindTaxaWithSufficientCount(beam.DoFn):
    def __init__(self, minimum_taxon_count, project):
        super(UnwindTaxaWithSufficientCount, self).__init__()
        from apache_beam.metrics import Metrics
        from google.cloud import storage

        self.sufficient_taxa_counter = Metrics.counter('main', 'sufficient_taxa')
        self.insufficient_taxa_counter = Metrics.counter('main', 'insufficient_taxa')
        self.minimum_taxon_count = minimum_taxon_count
        self.NORTHERNMOST = 49.
        self.SOUTHERNMOST = 25.
        self.EASTERNMOST = -66.
        self.WESTERNMOST = -124.

        client = storage.Client(project=project)
        bucket = client.get_bucket('floracast-conf')
        content = storage.Blob('dataflow.sh', bucket).download_as_string()
        for line in content.split(b'\n'):
            if "FLORACAST_GOOGLE_MAPS_API_KEY" in line.decode("utf-8"):
                self.FLORACAST_GOOGLE_MAPS_API_KEY = line.decode("utf-8").split("=")[1]

    def random_point(self):
        import random
        from datetime import datetime
        from pygeocoder import Geocoder, GeocoderError

        date = datetime(
            random.randint(2015, 2017),
            random.randint(1, 12),
            random.randint(1, 28)
        )
        while True:
            lat = round(random.uniform(self.SOUTHERNMOST, self.NORTHERNMOST), 6)
            lng = round(random.uniform(self.EASTERNMOST, self.WESTERNMOST), 6)
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

    def isclose(self, a, b, rel_tol=1e-09, abs_tol=0.0):
        return abs(a-b) <= max(rel_tol * max(abs(a), abs(b)), abs_tol)

    def group_list(self, l, group_size):
        """
        :param l:           list
        :param group_size:  size of each group
        :return:            Yields successive group-sized lists from l.
        """
        for i in xrange(0, len(l), group_size):
            yield l[i:i+group_size]

    def process(self, element):
        import tensorflow as tf
        from googlemaps import Client
        import mgrs

        taxon_list = list(element[1])
        if len(taxon_list) <= self.minimum_taxon_count:
            self.insufficient_taxa_counter.inc()
            return

        self.sufficient_taxa_counter.inc()

        locations = []
        blanks = []
        for o in taxon_list:
            yield o

            lat, lng, date = self.random_point()
            locations.append((lat, lng))
            se = tf.train.SequenceExample()
            se.context.feature["label"].int64_list.value.append(0)
            se.context.feature["latitude"].float_list.value.append(lat)
            se.context.feature["longitude"].float_list.value.append(lng)
            se.context.feature["date"].int64_list.value.append(int(date.strftime("%s")))
            se.context.feature["grid-zone"].bytes_list.value.append(mgrs.MGRS().toMGRS(lat, lng)[:2].encode())
            blanks.append(se)

        client = Client(key=self.FLORACAST_GOOGLE_MAPS_API_KEY)

        # Group here in order to run wider batches
        for batch in self.group_list(locations, 300):
            for r in client.elevation(batch):
                for i, b in enumerate(blanks):
                    if not self.isclose(
                            r['location']['lat'],
                            b.context.feature['latitude'].float_list.value[0],
                            abs_tol=0.00001
                    ):
                        continue
                    if not self.isclose(
                            r['location']['lng'],
                            b.context.feature['longitude'].float_list.value[0],
                            abs_tol=0.00001
                    ):
                        continue
                    blanks[i].context.feature["elevation"].float_list.value.append(r['elevation'])
                    break

        for b in blanks:
            yield b


# Filter and prepare for duplicate sort.
@beam.typehints.with_input_types(tf.train.SequenceExample)
@beam.typehints.with_output_types(str)
class EncodeExample(beam.DoFn):
    def __init__(self):
        super(EncodeExample, self).__init__()
        from apache_beam.metrics import Metrics
        self.final_occurrence_count = Metrics.counter('main', 'final_occurrence_count')

    def process(self, ex):
        if "elevation" not in ex.context.feature:
            return
        self.final_occurrence_count.inc()
        yield ex.SerializeToString()


@beam.typehints.with_input_types(str)
@beam.typehints.with_output_types(beam.typehints.KV[int, tf.train.SequenceExample])
class StringToTaxonSequenceExample(beam.DoFn):
    def __init__(self):
        super(StringToTaxonSequenceExample, self).__init__()
        from apache_beam.metrics import Metrics
        self.following_removed_duplicates = Metrics.counter('main', 'following_removed_duplicates')

    def process(self, element):
        import mgrs
        import tensorflow as tf

        self.following_removed_duplicates.inc()

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
        se.context.feature["grid-zone"].bytes_list.value.append(mgrs.MGRS().toMGRS(lat, lng)[:2].encode())

        yield taxon, se


@beam.typehints.with_input_types(tf.train.SequenceExample)
@beam.typehints.with_output_types(tf.train.SequenceExample)
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

            # q = """
            #       SELECT
            #         lat,
            #         lon,
            #         prcp,
            #         min,
            #         max,
            #         temp,
            #         mo,
            #         da,
            #         year,
            #         elev
            #       FROM
            #         [bigquery-public-data:noaa_gsod.gsod@year] a
            #       JOIN
            #         [bigquery-public-data:noaa_gsod.stations] b
            #       ON
            #         a.stn=b.usaf
            #         AND a.wban=b.wban
            #       WHERE
            #          lat <= @n_lat AND lat >= @s_lat
            #         AND lon >= @w_lon AND lon <= @e_lon
            #         AND @month_query
            #       ORDER BY
            #         da DESC
            # """
            # sync_query = client.run_sync_query(
            #     query=q,
            #     query_parameters=(
            #         bigquery.ScalarQueryParameter('n_lat', 'STRING', str(nw.latitude)),
            #         bigquery.ScalarQueryParameter('w_lon', 'STRING', str(nw.longitude)),
            #         bigquery.ScalarQueryParameter('s_lat', 'STRING', str(se.latitude)),
            #         bigquery.ScalarQueryParameter('e_lon', 'STRING', str(se.longitude)),
            #         bigquery.ScalarQueryParameter('year', 'STRING', str(year)),
            #         bigquery.ScalarQueryParameter('month_query', 'STRING', month_query)
            #     )
            # )

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


def datastore_query():
    from google.cloud.proto.datastore.v1 import query_pb2
    # q = query.Query(kind='Occurrence', project=project)
    # q.fetch()
    q = query_pb2.Query()
    q.kind.add().name='Occurrence'
    # datastore_helper.set_kind(q, 'Occurrence')
    # q.limit.name = 100

    # Need to index this in google.

    # datastore_helper.set_composite_filter(q.filter, CompositeFilter.AND,
    # datastore_helper.set_property_filter(ds.Filter(), 'Location.Lat', PropertyFilter.GREATER_THAN_OR_EQUAL, 5.4995),
    # datastore_helper.set_property_filter(ds.Filter(), 'Location.Lat', PropertyFilter.LESS_THAN_OR_EQUAL, 83.1621),
    # datastore_helper.set_property_filter(ds.Filter(), 'Location.Lng', PropertyFilter.GREATER_THAN_OR_EQUAL, -167.2764),
    # https://stackoverflow.com/questions/41705870/geospatial-query-at-google-app-engine-datastore
    # The query index may not be implemented at this point.
    # datastore_helper.set_property_filter(datastore.Filter(), 'Location.longitude', PropertyFilter.LESS_THAN_OR_EQUAL, -52.2330)
    # )
    return q


def run():
    from apache_beam.io.gcp.datastore.v1.datastoreio import ReadFromDatastore
    from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions
    # from apache_beam.metrics import MetricsFilter
    # parser = argparse.ArgumentParser()

    # Set defaults
    pipeline_options = PipelineOptions(
        #['--taxon', 58583]
    )

    google_cloud_options = pipeline_options.view_as(GoogleCloudOptions)
    occurrence_pipeline_options = pipeline_options.view_as(OccurrencePipelineOptions)
    standard_options = pipeline_options.view_as(StandardOptions)

    with beam.Pipeline(options=google_cloud_options) as p:

        (p
            | 'ReadDatastoreOccurrences' >> ReadFromDatastore(
                project=google_cloud_options.project,
                query=datastore_query(),
                num_splits=0
            )
            | 'ConvertEntitiesToKeyStrings' >> beam.ParDo(EntityToString(occurrence_pipeline_options.taxon))
            | 'RemoveDuplicates' >> beam.RemoveDuplicates()
            | 'ConvertKeyStringsToTaxonSequenceExample' >> beam.ParDo(StringToTaxonSequenceExample())
            | 'GroupByTaxon' >> beam.GroupByKey()
            | 'UnwindSufficientTaxa' >> beam.ParDo(UnwindTaxaWithSufficientCount(
                occurrence_pipeline_options.minimum_occurrences_within_taxon,
                google_cloud_options.project
            ))
            | 'FetchWeather' >> beam.ParDo(FetchWeatherDoFn(
                    google_cloud_options.project,
                    occurrence_pipeline_options.weather_station_distance
                ))
            | 'EncodeForWrite' >> beam.ParDo(EncodeExample())
            | 'Write' >> beam.io.WriteToTFRecord(
                    file_path_prefix=occurrence_pipeline_options.output,
                    file_name_suffix='.tfrecord',
                )
         )
        #
        # result = p.run()
        # result.wait_until_finish()
        # #
        # # # Print metrics if local runner.
        # if standard_options.runner == "DirectRunner":
        #
        #     for v in [
        #         'new_occurrences',
        #         'invalid_occurrence_elevation',
        #         'invalid_occurrence_location',
        #         'invalid_occurrence_date',
        #         'following_removed_duplicates',
        #         'sufficient_taxa',
        #         'insufficient_taxa',
        #         'insufficient_weather_records',
        #         'final_occurrence_count',
        #     ]:
        #         query_result = result.metrics().query(MetricsFilter().with_name(v))
        #         if query_result['counters']:
        #             logging.info('%s: %d', v, query_result['counters'][0].committed)


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()