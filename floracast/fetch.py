# from __future__ import absolute_import
from __future__ import division

import logging

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, DirectOptions, GoogleCloudOptions, StandardOptions, SetupOptions
from functions import fetch
from functions.weather import FetchWeatherDoFn
from functions.write import ExampleRecordWriter
from functions.example import Example
from datetime import datetime

# If error after upgrading apache beam: metaclass conflict: the metaclass of a derived class must be a (non-strict) subclass of the metaclasses of all its bases
# then: pip install six==1.10.0

logging.basicConfig()
logging.getLogger().setLevel(logging.DEBUG)

class LocalPipelineOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):


        #### FETCH ####

        # Intermediate TFRecords are stored in their own directory, each with a corresponding metadata file.
        # The metadata lists how many records, how many of each taxon label.
        parser.add_value_provider_argument(
            '--bucket',
            required=True,
            help='The GCS bucket to store TFRecord files'
        )

        parser.add_value_provider_argument(
            '--name_usages',
            required=False,
            default=None,
            type=str,
            help='NameUsages for which to fetch occurrences')


        parser.add_value_provider_argument(
            '--random',
            required=False,
            default=False,
            type=bool,
            help='Restrict example fetch to this taxon')

        parser.add_value_provider_argument(
            '--protected_areas',
            required=False,
            default=False,
            type=bool,
            help='Include ProtectedAreas in fetch')

        parser.add_value_provider_argument(
            '--protected_area_dates',
            required=False,
            default=None,
            type=str,
            help='A comma separated list of dates (YYYYMMDD) on which to search for protected areas.')

        parser.add_value_provider_argument(
            '--max_weather_station_distance',
            required=False,
            default=100,
            help='Maximum distance a weather station can be from an occurrence when fetching weather.')

def default_project():
    import os
    import subprocess
    get_project = [
        'gcloud', 'config', 'list', 'project', '--format=value(core.project)'
    ]

    with open(os.devnull, 'w') as dev_null:
        return subprocess.check_output(get_project, stderr=dev_null).strip()

def run(argv=None):

    pipeline_options = DirectOptions()

    standard_options = pipeline_options.view_as(StandardOptions)

    local_pipeline_options = pipeline_options.view_as(LocalPipelineOptions)

    cloud_options = pipeline_options.view_as(GoogleCloudOptions)
    cloud_options.project = default_project()

    pipeline_options.view_as(SetupOptions).save_main_session = True

    with beam.Pipeline(standard_options.runner, options=pipeline_options) as pipeline:
            random = pipeline \
                     | 'SetRandomInMotion' >> beam.Create([1]).with_output_types(int) \
                     | 'GenerateRandomOccurrenceBatches' >> beam.ParDo(fetch.GeneratePointBatches(
                            project=cloud_options.project,
                            collection="Random",
                            engage=local_pipeline_options.random
                        )) \
                     | 'FetchRandom' >> beam.ParDo(
                            fetch.FetchRandom(project=cloud_options.project)
                        )

            protected_areas = pipeline \
                              | 'SetProtectedAreasInMotion' >> beam.Create([1]).with_output_types(int) \
                              | 'GenerateProtectedAreaBatches' >> beam.ParDo(fetch.GeneratePointBatches(
                                    project=cloud_options.project,
                                    collection='ProtectedAreas',
                                    engage=local_pipeline_options.protected_areas
                                )) \
                              | 'FetchProtectedAreas' >> beam.ParDo(
                                    fetch.FetchProtectedAreas(project=cloud_options.project)
                                ) \
                              | 'ExplodeProtectedAreaDates' >> beam.ParDo(
                                    fetch.ExplodeProtectedAreaDates(
                                        protected_area_dates=local_pipeline_options.protected_area_dates
                                    )
                                )

            # occurrences = pipeline \
            #               | 'SetOccurrencesInMotion' >> beam.Create([1]).with_output_types(int) \
            #               | 'FetchNameUsages' >> beam.ParDo(
            #                     fetch.FetchNameUsages(
            #                         project=cloud_options.project,
            #                         nameusages=local_pipeline_options.name_usages
            #                     )
            #                 ) \
            #               | 'FetchOccurrences' >> beam.ParDo(fetch.FetchOccurrences(cloud_options.project))

            examples = (protected_areas, random) | beam.Flatten()

            _ = examples \
                   | 'ProjectSeasonRegionKV' >> beam.Map(
                            lambda e: (e.season_region_key(), e)
                    ).with_input_types(Example).with_output_types(beam.typehints.KV[str, Example]) \
                   | 'GroupSeasonRegion' >> beam.GroupByKey().with_input_types(beam.typehints.KV[str, Example]).with_output_types(beam.typehints.KV[str, beam.typehints.Iterable[Example]]) \
                   | 'FetchWeather' >> beam.ParDo(
                        FetchWeatherDoFn(project=cloud_options.project)
                      ) \
                   | 'ProjectCategoryKV' >> beam.Map(
                        lambda e: (e.pipeline_category(), e)
                    ).with_input_types(Example).with_output_types(beam.typehints.KV[str, Example]) \
                   | 'GroupByCategory' >> beam.GroupByKey().with_input_types(beam.typehints.KV[str, Example]).with_output_types(beam.typehints.KV[str, beam.typehints.Iterable[Example]]) \
                   | 'WriteRecords' >> beam.ParDo(
                        ExampleRecordWriter(
                            project=cloud_options.project,
                            bucket=local_pipeline_options.bucket,
                            timestamp=datetime.now().strftime("%s"),
                        )
                    )

                    # | 'ProtoForWrite' >> beam.Map(
                    #      lambda e: (e.category(), e.encode())
                    #  ).with_input_types(Example).with_output_types(beam.typehints.Tuple[str, Example]) \
                   # | 'DivideByTaxon' >> beam.ParDo(_DivideByTaxon()).with_outputs(*taxa_list)

            # for taxon in taxa_list:

            # _ = occurrences_by_taxon[taxon] \
            #     | ('ShuffleOccurrences[%s]' % taxon) >> utils.Shuffle() \
            #     | ('ProtoForWrite[%s]' % taxon) >> beam.Map(lambda e: e.encode()) \
            #     | ('WriteOccurrences[%s]' % taxon) >> beam.io.WriteToTFRecord(local_pipeline_options.data_location, file_name_suffix='.tfrecord.gz')


# @beam.typehints.with_input_types(ex.Example)
# @beam.typehints.with_output_types(ex.Example)
# class _DivideByTaxon(beam.DoFn):
#     def __init__(self):
#         super(_DivideByTaxon, self).__init__()
#     def process(self, example):
#         yield beam.pvalue.TaggedOutput(example.taxon(), example)



if __name__ == '__main__':
    # logging.getLogger().setLevel(logging.DEBUG)
    run()