# from __future__ import absolute_import
import apache_beam as beam
from example import Example
# from google.cloud.proto.datastore.v1 import entity_pb2
from apache_beam.transforms.core import PTransform
from apache_beam.io import iobase


def fetch_occurrences(
        pipeline_options,
        output_path,
):
    import apache_beam as beam
    import weather as weather
    import elevation as elevation
    from apache_beam.io import WriteToText
    # from apache_beam import pvalue
    import utils as utils
    from tensorflow_transform.beam import impl as tft

    options = pipeline_options.get_all_options()

    with beam.Pipeline(options['runner'], options=pipeline_options) as pipeline:
        with tft.Context(temp_dir=options['temp_location']):

            taxa = pipeline \
                   | 'ReadTaxa' >> _ReadTaxa(
                        project=options['project'],
                        taxa=options['occurrence_taxa'],
                        ecoregion=options['ecoregion'],
                        minimum_occurrences_within_taxon=options['minimum_occurrences_within_taxon']
                   )

            occurrences = taxa \
                   | 'FetchOccurrences' >> beam.ParDo(_FetchOccurrences(options['project'])) \
                   | 'RemoveOccurrenceExampleLocationDuplicates' >> utils.RemoveOccurrenceExampleLocationDuplicates()

            # occurrence_count = occurrences | beam.combiners.Count.Globally()
            #
            # if options['add_random_train_point'] is True:
            #     random_examples = occurrence_count | 'AddRandomTrainPoints' >> beam.ParDo(_AddRandomTrainPoints())
            #     occurrences = (occurrences, random_examples) | beam.Flatten()

            occurrences = occurrences \
                 | 'GroupByYearMonth' >> utils.GroupByYearMonth() \
                 | 'FetchWeather' >> beam.ParDo(weather.FetchWeatherDoFn(options['project'], options['weather_station_distance'])) \
                 | 'EnsureElevation' >> beam.ParDo(elevation.ElevationBundleDoFn(options['project'])) \
                 | 'ShuffleOccurrences' >> utils.Shuffle() \
                 | 'ProtoForWrite' >> beam.Map(lambda e: e.encode())

            _ = occurrences \
                | 'WriteOccurrences' >> beam.io.WriteToTFRecord(output_path + "/", file_name_suffix='.tfrecord.gz')

            # _ = data \
            #     | 'EncodePredictAsB64Json' >> beam.Map(utils.encode_as_b64_json) \
            #     | 'WritePredictDataAsText' >> beam.io.WriteToText(output_path, file_name_suffix='.txt')


            _ = taxa | 'WriteTaxa' >> beam.io.WriteToText(output_path + "/", file_name_suffix='taxa.txt')

            # Write metadata
            _ = pipeline | beam.Create([{
                'weather_station_distance': options['weather_station_distance'],
                'minimum_occurrences_within_taxon': options['minimum_occurrences_within_taxon'],
                'random_train_points': options['add_random_train_point']
            }]) \
                | 'WriteToMetadataFile' >> WriteToText(output_path + "/", file_name_suffix=".query.meta", num_shards=1)


class ComputeWordLengths(beam.PTransform):
    def expand(self, pcoll):
        # transform logic goes here
        return pcoll | beam.Map(lambda x: len(x))

@beam.typehints.with_input_types(int)
@beam.typehints.with_output_types(Example)
class _AddRandomTrainPoints(beam.DoFn):
    def __init__(self):
        super(_AddRandomTrainPoints, self).__init__()

    def process(self, total_occurrence_count):
        import example as example
        import math
        if total_occurrence_count == 0:
            return
        random_example_count = total_occurrence_count * 0.20
        if random_example_count < 700:
            if total_occurrence_count <= 700:
                random_example_count = total_occurrence_count
            else:
                random_example_count = 700
        for _ in range(0, int(math.floor(random_example_count))):
            yield example.RandomExample()
#
# # Filter and prepare for duplicate sort.
# @beam.typehints.with_input_types(dict)
# @beam.typehints.with_output_types(Example)
# class _OccurrenceEntityToExample(beam.DoFn):
#
#     def __init__(self, taxon=0):
#         from apache_beam.metrics import Metrics
#         super(_OccurrenceEntityToExample, self).__init__()
#         self.new_occurrence_counter = Metrics.counter('main', 'new_occurrences')
#         self.invalid_occurrence_date = Metrics.counter('main', 'invalid_occurrence_date')
#         self.invalid_occurrence_elevation = Metrics.counter('main', 'invalid_occurrence_elevation')
#         self.invalid_occurrence_location = Metrics.counter('main', 'invalid_occurrence_location')
#
#     def process(self, e):
#         # from google.cloud.datastore.helpers import entity_from_protobuf, GeoPoint
#         # from google.cloud.datastore import Entity
#         import logging
#         """
#             Element should be an occurrence entity.
#             The key has should be a sufficient key.
#         """
#         # e = entity_from_protobuf(element)
#
#         self.new_occurrence_counter.inc()
#
#         # This is a hack to avoid indexing the 'Date' property in Go.
#         if e['Date'].year < 1970:
#             self.invalid_occurrence_date.inc()
#             return
#
#         if 'Elevation' not in e:
#             self.invalid_occurrence_elevation.inc()
#             return
#
#         lat = e['Location']['Latitude']
#         lng = e['Location']['Longitude']
#         elevation = e['Elevation']
#
#         # (lat, lng) = (0.0, 0.0)
#         # if type(loc) is GeoPoint:
#         #     lat = loc.latitude
#         #     lng = loc.longitude
#         # elif type(loc) is Entity:
#         #     lat = loc['Lat']
#         #     lng = loc['Lng']
#         # else:
#         #     logging.error("invalid type: %s", type(loc))
#         #     return
#
#         if lng > -52.2330:
#             logging.info("%.6f && %.6f", lat, lng)
#             self.invalid_occurrence_location.inc()
#             return
#
#         ex = Example()
#         ex.set_date(int(e['Date'].strftime("%s")))
#         ex.set_latitude(lat)
#         ex.set_longitude(lng)
#         ex.set_elevation(elevation)
#         ex.set_taxon(e["TaxonID"])
#         ex.set_occurrence_id(e["OccurrenceID"])
#
#         yield ex


# class Counter(beam.DoFn):
#     def __init__(self, which):
#         self._which = which
#         self._counter = 0
#
#     def process(self, element):
#         self._counter += 1
#         yield element

# @beam.typehints.with_input_types(Example)
# @beam.typehints.with_output_types(Example)
# class _RemoveScantTaxa(beam.PTransform):
#     """Count as a subclass of PTransform, with an apply method."""
#
#     def __init__(self, minimum_occurrences_within_taxon):
#         self._minimum_occurrences_within_taxon = minimum_occurrences_within_taxon
#
#     def expand(self, pcoll):
#         return pcoll \
#                 | 'ExamplesToTaxonTuples' >> beam.Map(lambda e: (e.taxon(), e)) \
#                 | 'GroupByTaxon' >> beam.GroupByKey() \
#                 | 'FilterAndUnwindOccurrences' >> beam.FlatMap(
#                     lambda (taxon, occurrences): occurrences if len(list(occurrences)) >= self._minimum_occurrences_within_taxon else [])


# Unsure if this is the right model rather than a BoundedSource, but as my source is already
# Unsplittable, I think it should be fine to fetch occurrences individually for each given TaxonID.
# We can even convert to Example here to save a step.
@beam.typehints.with_input_types(str)
@beam.typehints.with_output_types(Example)
class _FetchOccurrences(beam.DoFn):
    def __init__(self, project):
        super(_FetchOccurrences, self).__init__()
        self._project = project

    def process(self, taxon_id):
        from google.cloud.firestore_v1beta1 import client
        db = client.Client(project=self._project)
        q = db.collection(u'Occurrences')
        for o in q.where(u'TaxonID', u'==', taxon_id).order_by("FormattedDate", "DESCENDING").limit(500).get():
            # self.records_read.inc()
            taxon = o.to_dict()

            # This is a hack to avoid indexing the 'Date' property in Go.
            if taxon['Date'].year < 1970:
                continue

            if 'Elevation' not in taxon:
                continue

            lat = taxon['Location']['Latitude']
            lng = taxon['Location']['Longitude']
            elevation = taxon['Elevation']

            # (lat, lng) = (0.0, 0.0)
            # if type(loc) is GeoPoint:
            #     lat = loc.latitude
            #     lng = loc.longitude
            # elif type(loc) is Entity:
            #     lat = loc['Lat']
            #     lng = loc['Lng']
            # else:
            #     logging.error("invalid type: %s", type(loc))
            #     return

            ex = Example()
            ex.set_date(int(taxon['Date'].strftime("%s")))
            ex.set_latitude(lat)
            ex.set_longitude(lng)
            ex.set_elevation(elevation)
            ex.set_taxon(taxon_id)
            ex.set_occurrence_id(str(taxon['DataSourceID']+"|"+taxon['TargetID']))

            yield ex

class TaxaSource(iobase.BoundedSource):

    def __init__(self, project, taxa="", ecoregion="", minimum_occurrences_within_taxon=100):
        # from apache_beam.metrics import Metrics
        # self.records_read = Metrics.counter(self.__class__, 'recordsRead')
        self._project = project
        if taxa != None:
            self._taxa = taxa.split(",")
        else:
            self._taxa = []
        self._ecoregion = ecoregion
        self._minimum_occurrences_within_taxon = minimum_occurrences_within_taxon

    def estimate_size(self):
        return 500

    def get_range_tracker(self, start_position, stop_position):
        from apache_beam.io import range_trackers
        if start_position is None:
            start_position = 0
        if stop_position is None:
            stop_position = range_trackers.OffsetRangeTracker.OFFSET_INFINITY

        # Use an unsplittable range tracker. This means that a collection can
        # only be read sequentially for now.
        range_tracker = range_trackers.OffsetRangeTracker(start_position, stop_position)
        range_tracker = range_trackers.UnsplittableRangeTracker(range_tracker)

        return range_tracker

    def read(self, range_tracker):
        from google.cloud.firestore_v1beta1 import client

        # One of the two are required.
        if self._ecoregion == "" and len(self._taxa) == 0:
            return

        db = client.Client(project=self._project)

        if self._ecoregion != "":

            taxa = dict()
            # subfield = ("EcoRegions.%s" % self._ecoregion)
            path = db.field_path("EcoRegions", "_%s" % self._ecoregion)
            for o in db.collection(u'Taxa').where(path, ">", 0).get():
                d = o.to_dict()

                total_occurrences = 0
                total_ecoregions = 0
                occurrences_in_this_ecoregion = 0

                for k in d["EcoRegions"]:
                    if k == self._ecoregion:
                        occurrences_in_this_ecoregion = int(d["EcoRegions"][k])
                    total_ecoregions = total_ecoregions + 1
                    total_occurrences = total_occurrences + int(d["EcoRegions"][k])

                if total_occurrences < self._minimum_occurrences_within_taxon:
                    continue

                taxa[d["ID"]] = ((occurrences_in_this_ecoregion/total_occurrences)/total_ecoregions)

            # Sort descending
            count = 0
            for key, value in sorted(taxa.iteritems(), key=lambda (k,v): (v,k)):
                yield key
                count = count + 1
                # Limit each model to 1000 taxa.
                if count >= 1000:
                    return

            return

        # for t in self._taxa:
        #     yield self._taxa
            # q = db.collection(u'Occurrences')
            # for o in q.where(u'TaxonID', u'==', t).get():
            #     # self.records_read.inc()
            #     yield o.to_dict()

    def split(self, desired_bundle_size, start_position=None, stop_position=None):
        """Implements :class:`~apache_beam.io.iobase.BoundedSource.split`
        This function will currently not be called, because the range tracker
        is unsplittable
        """
        from apache_beam.io import range_trackers

        if start_position is None:
            start_position = 0
        if stop_position is None:
            stop_position = range_trackers.OffsetRangeTracker.OFFSET_INFINITY

        # Because the source is unsplittable (for now), only a single source is
        # returned.
        yield iobase.SourceBundle(
            weight=1,
            source=self,
            start_position=start_position,
            stop_position=stop_position)

class _ReadTaxa(PTransform):
    """A :class:`~apache_beam.transforms.ptransform.PTransform` for reading
    from MongoDB.
    """
    def __init__(self, project, taxa, ecoregion, minimum_occurrences_within_taxon):
        """Initializes :class:`ReadFromMongo`
        Uses source :class:`_MongoSource`
        """
        super(_ReadTaxa, self).__init__()
        self._source = TaxaSource(
            project=project,
            taxa=taxa,
            ecoregion=ecoregion,
            minimum_occurrences_within_taxon=minimum_occurrences_within_taxon)

    def expand(self, pcoll):
        """Implements :class:`~apache_beam.transforms.ptransform.PTransform.expand`"""
        return pcoll | iobase.Read(self._source)

    # def display_data(self):
    #     return {'source_dd': self._source}