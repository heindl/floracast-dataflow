from apache_beam.options.pipeline_options import PipelineOptions

def _default_project():
    import os
    import subprocess
    get_project = [
        'gcloud', 'config', 'list', 'project', '--format=value(core.project)'
    ]

    with open(os.devnull, 'w') as dev_null:
        return subprocess.check_output(get_project, stderr=dev_null).strip()

class ProcessPipelineOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):


        #### GENERAL ####


        parser.add_argument(
            '--mode',
            required=True,
            help='eval, train, or infer which defines the pipeline to run.')


        #### FETCH ####

        # The intermediate filenames are unix timestamps.
        # If not specified new occurrence data will be fetched and a new timestamp given.
        parser.add_argument(
            '--raw_data_location',
            required=False,
            default=None,
            help='A unix timestamp representing the fetched time of occurrences, the directory in which tfrecords are stored.'
        )

        parser.add_argument(
            '--minimum_occurrences_within_taxon',
            required=False,
            default=40,
            help='The number of occurrence required to process taxon')

        parser.add_argument(
            '--occurrence_taxa',
            required=False,
            default=None,
            type=list,
            help='Restrict occurrence fetch to this taxa')

        parser.add_argument(
            '--weather_station_distance',
            required=False,
            default=75,
            help='Maximum distance a weather station can be from an occurrence when fetching weather.')


        #### TRAIN ####

        parser.add_argument(
            '--percent_eval',
            required=False,
            default=10,
            type=int,
            help='Directory that contains timestamped files for each training iteration')

        parser.add_argument(
            '--train_location',
            required=False,
            help='Directory that contains timestamped files for each training iteration')


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
            '--infer_location',
            required=False,
            help='Directory that contains timestamped files for collected infer data. Should be similar meta to fetched format.')

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
        # pip install "apache_beam[gcp]"