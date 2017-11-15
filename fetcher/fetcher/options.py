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
            '--minimum_occurrences_within_taxon',
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
            '--weather_station_distance',
            required=False,
            default=150,
            help='Maximum distance a weather station can be from an occurrence when fetching weather.')

        parser.add_argument(
            '--infer_location',
            required=False,
            help='Directory that contains timestamped files for protected areas')

        #### TRAIN ####
        parser.add_argument(
            '--train_location',
            required=False,
            help='Directory that contains timestamped files for each training iteration')

        # parser.add_argument(
        #     '--ecoregion',
        #     required=False,
        #     type=str,
        #     help='Number of eco regions')

        # Google cloud options.
        # parser.add_argument(
        #     '--temp_location',
        #     required=True,
        #     help='Temporary data')
        #
        # parser.add_argument(
        #     '--staging_location',
        #     required=True,
        #     help='Staging data')

        parser.add_argument(
            '--num_classes',
            required=False,
            type=int,
            help='Number of training classes')


        #### INFER ####


        parser.add_argument(
            '--weeks_before',
            required=False,
            default=1,
            type=int,
            help='The number of weeks in the past to generate prediction data for each forest'
            # If the model changes, we can expect this to be 52 weeks in the past. If not, just this week,
            # calculated every Friday.
        )

        parser.add_argument(
            '--protected_area_count',
            required=False,
            default=0,
            type=int,
            help='The number of locations to generate data for'
            # If the model changes, we can expect this to be 52 weeks in the past. If not, just this week,
            # calculated every Friday.
        )

        parser.add_argument(
            '--add_random_train_point',
            required=False,
            default=True,
            help='Should a random training location be added for every actual occurrence?')