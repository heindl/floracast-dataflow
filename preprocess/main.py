# from __future__ import absolute_import

import logging

from apache_beam.options.pipeline_options import PipelineOptions


# pip install "apache_beam[gcp]"

def _default_project():
    import subprocess
    import os

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

        # Intermediate TFRecords are stored in their own directory, each with a corresponding metadata file.
        # The metadata lists how many records, how many of each taxon label.
        parser.add_argument(
            '--intermediate_data',
            required=True,
            help='The intermediate TFRecords file that contains downloaded features from BigQuery'
        )

        # The intermediate filenames are unix timestamps.
        # If not specified new occurrence data will be fetched and a new timestamp given.
        parser.add_argument(
            '--intermediate_data_prefix',
            required=False,
            default=None,
            help='A unix timestamp representing the fetched time of an occurrence TFRecords file.'
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
            help='Restrict occurrence fetch to this taxa')

        parser.add_argument(
            '--weather_station_distance',
            required=False,
            default=75,
            help='Maximum distance a weather station can be from an occurrence when fetching weather.')


        #### TRAIN ####

        parser.add_argument(
            '--train_data',
            required=True,
            help='Directory that contains timestamped files for each training iteration')

        parser.add_argument(
            '--temp_data',
            required=True,
            help='Directory that contains timestamped files for each training iteration')

        parser.add_argument(
            '--num_classes',
            required=True,
            type=int,
            help='Number of training classes')

        #### INFER ####


        parser.add_argument(
            '--infer_data',
            required=True,
            help='Directory that contains timestamped files for collected infer data. Should be similar meta to fetched format.')

        parser.add_argument(
            '--weeks_before',
            required=False,
            default=1,
            help='The number of weeks in the past to generate prediction data for each forest'
            # If the model changes, we can expect this to be 52 weeks in the past. If not, just this week,
            # calculated every Friday.
        )

        parser.add_argument(
            '--add_random_train_point',
            required=False,
            default=True,
            help='Should a random training location be added for every actual occurrence?')


def main(argv=None):
    """Run Preprocessing as a Dataflow."""
    from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions
    from tensorflow_transform.beam import impl as tft
    import tensorflow as tf
    import preprocess
    import datetime
    import apache_beam as beam
    import os


    RAW_METADATA_DIR = 'raw_metadata'
    TRANSFORMED_PREDICT_DATA_FILE_PREFIX = 'features_predict'

    pipeline_options = PipelineOptions(
        #['--taxon', 58583]
    )

    process_pipeline_options = pipeline_options.view_as(ProcessPipelineOptions)
    google_cloud_options = pipeline_options.view_as(GoogleCloudOptions)
    standard_options = pipeline_options.view_as(StandardOptions)

    with beam.Pipeline(standard_options.runner, options=google_cloud_options) as pipeline:
        with tft.Context(temp_dir=process_pipeline_options.temp_data):

            if process_pipeline_options.mode == tf.contrib.learn.ModeKeys.TRAIN:

                intermediate_records=""

                if process_pipeline_options.intermediate_data_prefix is not None:
                    intermediate_records = os.path.join(
                        process_pipeline_options.intermediate_data,
                        process_pipeline_options.intermediate_data_prefix,
                    )
                else:
                    intermediate_records = os.path.join(
                        process_pipeline_options.intermediate_data,
                        datetime.datetime.now().strftime("%s"),
                    )

                # If specified, first generate intermediate tfrecords with raw source data, with no transformations applied.
                # This can be reused with modified transformations without incurring BigQuery cost.
                if process_pipeline_options.intermediate_data_prefix is None:

                    preprocess.fetch_train(
                        pipeline=pipeline,
                        records_file_path=intermediate_records,
                        mode=process_pipeline_options.mode,
                        project=google_cloud_options.project,
                        occurrence_taxa=process_pipeline_options.occurrence_taxa,
                        weather_station_distance=process_pipeline_options.weather_station_distance,
                        minimum_occurrences_within_taxon=process_pipeline_options.minimum_occurrences_within_taxon,
                        add_random_train_point=process_pipeline_options.add_random_train_point
                    )

                    return

                train_directory_path = os.path.join(process_pipeline_options.train_data, datetime.datetime.now().strftime("%s"))

                preprocess.preprocess_train(
                    pipeline,
                    intermediate_records=intermediate_records,
                    mode=process_pipeline_options.mode,
                    output_path=train_directory_path,
                    partition_random_seed=int(datetime.datetime.now().strftime("%s")),
                    percent_eval=10,
                    num_classes=process_pipeline_options.num_classes,
                )

            elif process_pipeline_options.mode == tf.contrib.learn.ModeKeys.INFER:

                infer_directory_path = os.path.join(process_pipeline_options.infer_data, datetime.datetime.now().strftime("%s"))

                preprocess.preprocess_infer(
                    pipeline=pipeline,
                    project=google_cloud_options.project,
                    metadata_path=os.path.join(infer_directory_path, RAW_METADATA_DIR),
                    data_path=os.path.join(infer_directory_path, TRANSFORMED_PREDICT_DATA_FILE_PREFIX),
                    weeks_before=process_pipeline_options.weeks_before,
                    weather_station_distance=process_pipeline_options.weather_station_distance
                )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    main()