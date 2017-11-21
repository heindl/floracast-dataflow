# from __future__ import absolute_import

from apache_beam.options.pipeline_options import PipelineOptions

class ProcessPipelineOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):

        parser.add_argument(
            '--mode',
            required=True,
            help='eval, train, or infer which defines the pipeline to run.')

        #### FETCH ####

        # Intermediate TFRecords are stored in their own directory, each with a corresponding metadata file.
        # The metadata lists how many records, how many of each taxon label.
        parser.add_argument(
            '--intermediate_location',
            required=False,
            help='The intermediate TFRecords file that contains downloaded features from BigQuery'
        )

        parser.add_argument(
            '--random_location',
            required=False,
            help='The TFRecords directory that random occurrences created for classifying the model.'
        )

        parser.add_argument(
            '--random_occurrence_count',
            default=0,
            type=int,
            required=False,
            help='The number of random occurrences to fetcher for testing.'
        )

        parser.add_argument(
            '--min_occurrences_within_taxon',
            required=False,
            default=100,
            help='The number of occurrence required to process taxon')

        parser.add_argument(
            '--occurrence_taxa',
            required=False,
            default=None,
            type=str,
            help='Restrict occurrence fetch to this taxa')

        parser.add_argument(
            '--max_weather_station_distance',
            required=False,
            default=100,
            help='Maximum distance a weather station can be from an occurrence when fetching weather.')

        #### INFER ####

        parser.add_argument(
            '--protected_area_count',
            required=False,
            default=0,
            type=int,
            help='The number of areas to fetch. Gathers all if zero.'
        )

        parser.add_argument(
            '--date',
            required=False,
            type=str,
            help='The date on which to gather wilderness areas.'
        )