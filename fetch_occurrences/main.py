# from __future__ import absolute_import
from __future__ import division

import logging

import os
from shared import elevation, weather, utils
from tensorflow_transform.beam import impl as tft
from datetime import datetime as dt
from shared import ex
import apache_beam as beam
from apache_beam.transforms.core import PTransform
from apache_beam.io import iobase
from apache_beam.io import range_trackers
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions, SetupOptions
from google.cloud.firestore_v1beta1 import client


# If error after upgradeing apache beam: metaclass conflict: the metaclass of a derived class must be a (non-strict) subclass of the metaclasses of all its bases
# then: pip install six==1.10.0

class LocalPipelineOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):

        #### FETCH ####

        # Intermediate TFRecords are stored in their own directory, each with a corresponding metadata file.
        # The metadata lists how many records, how many of each taxon label.
        parser.add_argument(
            '--output_location',
            required=True,
            help='The intermediate TFRecords file that contains downloaded features from BigQuery'
        )

        parser.add_argument(
            '--min_occurrences_within_taxon',
            required=False,
            default=100,
            help='The number of occurrence required to process taxon')

        parser.add_argument(
            '--taxa',
            required=True,
            default=None,
            type=str,
            help='Restrict occurrence fetch to this taxa')

        parser.add_argument(
            '--max_weather_station_distance',
            required=False,
            default=100,
            help='Maximum distance a weather station can be from an occurrence when fetching weather.')


def run(argv=None):

    pipeline_options = PipelineOptions()

    local_pipeline_options = pipeline_options.view_as(LocalPipelineOptions)
    cloud_options = pipeline_options.view_as(GoogleCloudOptions)
    standard_options = pipeline_options.view_as(StandardOptions)
    pipeline_options.view_as(SetupOptions).setup_file = os.path.abspath(os.path.join(os.path.dirname(__file__), 'setup.py'))
    pipeline_options.view_as(SetupOptions).save_main_session = True

    t = dt.now().strftime("%s")

    with beam.Pipeline(standard_options.runner, options=pipeline_options) as pipeline:
        with tft.Context(temp_dir=cloud_options.temp_location):

            taxa_list = local_pipeline_options.occurrence_taxa.split(",")

            taxa = pipeline | 'ReadTaxa' >> _ReadTaxa(
                project=cloud_options.project,
                taxa=taxa_list,
                # ecoregion=options['ecoregion'], Have a different process that pulls an array for this.
                minimum_occurrences_within_taxon=local_pipeline_options.minimum_occurrences_within_taxon
            )

            occurrences = taxa \
                          | 'FetchOccurrences' >> beam.ParDo(_FetchOccurrences(cloud_options.project)) \
                          | 'RemoveOccurrenceExampleLocationDuplicates' >> utils.RemoveOccurrenceExampleLocationDuplicates()

            occurrences_by_taxon = occurrences \
                                   | 'GroupByYearMonth' >> utils.GroupByYearMonth() \
                                   | 'FetchWeather' >> beam.ParDo(weather.FetchWeatherDoFn(cloud_options.project, local_pipeline_options.max_weather_station_distance)) \
                                   | 'EnsureElevation' >> beam.ParDo(elevation.ElevationBundleDoFn(cloud_options.project)) \
                                   | 'DivideByTaxon' >> beam.ParDo(_DivideByTaxon()).with_outputs(*taxa_list)


            for taxon in taxa_list:

                path = local_pipeline_options.output_location + "/" + taxon + "/" + t + "/"

                if standard_options.runner == 'DirectRunner':
                    os.makedirs(path)

                _ = occurrences_by_taxon[taxon] \
                    | ('ShuffleOccurrences[%s]' % taxon) >> utils.Shuffle() \
                    | ('ProtoForWrite[%s]' % taxon) >> beam.Map(lambda e: e.encode()) \
                    | ('WriteOccurrences[%s]' % taxon) >> beam.io.WriteToTFRecord(path, file_name_suffix='.tfrecord.gz')


@beam.typehints.with_input_types(ex.Example)
@beam.typehints.with_output_types(ex.Example)
class _DivideByTaxon(beam.DoFn):
    def __init__(self):
        super(_DivideByTaxon, self).__init__()
    def process(self, example):
        yield beam.pvalue.TaggedOutput(example.taxon(), example)

# Unsure if this is the right model rather than a BoundedSource, but as my source is already
# Unsplittable, I think it should be fine to fetch occurrences individually for each given TaxonID.
# We can even convert to Example here to save a step.
@beam.typehints.with_input_types(str)
@beam.typehints.with_output_types(ex.Example)
class _FetchOccurrences(beam.DoFn):
    def __init__(self, project):
        super(_FetchOccurrences, self).__init__()
        self._project = project

    def process(self, taxon_id):
        db = client.Client(project=self._project)
        q = db.collection(u'Occurrences')

        for o in q.where(u'TaxonID', u'==', unicode(taxon_id, "utf-8")).order_by("FormattedDate", "DESCENDING").limit(500).get():

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

            e = ex.Example()
            e.set_date(int(taxon['Date'].strftime("%s")))
            e.set_latitude(lat)
            e.set_longitude(lng)
            e.set_elevation(elevation)
            e.set_taxon(taxon_id)
            e.set_occurrence_id(str(taxon['DataSourceID']+"|"+taxon['TargetID']))

            yield e

class _TaxaSource(iobase.BoundedSource):

    def __init__(self, project, taxa=[], ecoregion="", minimum_occurrences_within_taxon=100):
        self._project = project
        if taxa != None and len(taxa) > 0:
            self._taxa = taxa.split(",")
            # self._taxa = taxa
        else:
            self._taxa = []
        self._ecoregion = ecoregion
        self._minimum_occurrences_within_taxon = minimum_occurrences_within_taxon

    def estimate_size(self):
        return 500

    def get_range_tracker(self, start_position, stop_position):
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

        # One of the two are required.
        # if self._ecoregion == "" and len(self._taxa) == 0:
        if len(self._taxa) == 0:
            return

        db = client.Client(project=self._project)

        for taxon_id in self._taxa:
            for taxon in db.collection(u'Taxa').where(u'ID', u'==', unicode(taxon_id, "utf-8")).get():

                fields = taxon.to_dict()
                total_occurrences = 0

                if fields["EcoRegions"] is None:
                    continue

                for k in fields["EcoRegions"]:
                    total_occurrences = int(fields["EcoRegions"][k]) + total_occurrences

                if total_occurrences < self._minimum_occurrences_within_taxon:
                    continue

                yield taxon_id
                break

                # if self._ecoregion != "":
                #
                #     taxa = dict()
                #     # subfield = ("EcoRegions.%s" % self._ecoregion)
                #     path = db.field_path("EcoRegions", "_%s" % self._ecoregion)
                #     for o in db.collection(u'Taxa').where(path, ">", 0).get():
                #         d = o.to_dict()
                #
                #         total_occurrences = 0
                #         total_ecoregions = 0
                #         occurrences_in_this_ecoregion = 0
                #
                #         for k in d["EcoRegions"]:
                #             if k == self._ecoregion:
                #                 occurrences_in_this_ecoregion = int(d["EcoRegions"][k])
                #             total_ecoregions = total_ecoregions + 1
                #             total_occurrences = total_occurrences + int(d["EcoRegions"][k])
                #
                #         if total_occurrences < self._minimum_occurrences_within_taxon:
                #             continue
                #
                #         taxa[d["ID"]] = ((occurrences_in_this_ecoregion/total_occurrences)/total_ecoregions)
                #
                #     # Sort descending
                #     count = 0
                #     for key, value in sorted(taxa.iteritems(), key=lambda (k,v): (v,k)):
                #         yield key
                #         count = count + 1
                #         # Limit each model to 1000 taxa.
                #         if count >= 1000:
                #             return
                #
                #     return



    def split(self, desired_bundle_size, start_position=None, stop_position=None):
        """Implements :class:`~apache_beam.io.iobase.BoundedSource.split`
        This function will currently not be called, because the range tracker
        is unsplittable
        """

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
    def __init__(self, project, taxa, minimum_occurrences_within_taxon):
        """Initializes :class:`ReadFromMongo`
        Uses source :class:`_MongoSource`
        """
        super(_ReadTaxa, self).__init__()
        self._source = _TaxaSource(
            project=project,
            taxa=taxa,
            # ecoregion=ecoregion,
            minimum_occurrences_within_taxon=minimum_occurrences_within_taxon)

    def expand(self, pcoll):
        """Implements :class:`~apache_beam.transforms.ptransform.PTransform.expand`"""
        return pcoll | iobase.Read(self._source)

        # def display_data(self):
        #     return {'source_dd': self._source}


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()