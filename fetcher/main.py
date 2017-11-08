# from __future__ import absolute_import
from __future__ import division

import logging


# pip install "apache_beam[gcp]"

def _default_project():
    import os
    import subprocess
    get_project = [
        'gcloud', 'config', 'list', 'project', '--format=value(core.project)'
    ]

    with open(os.devnull, 'w') as dev_null:
        return subprocess.check_output(get_project, stderr=dev_null).strip()


def main(argv=None):
    from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions, SetupOptions
    # If error after upgradeing apache beam: metaclass conflict: the metaclass of a derived class must be a (non-strict) subclass of the metaclasses of all its bases
    # then: pip install six==1.10.0
    import tensorflow as tf
    import datetime
    import os
    from fetcher import options, occurrences, protected_areas, random_occurrences

    pipeline_options = PipelineOptions(flags=argv)
    # ['--setup_file', os.path.abspath(os.path.join(os.path.dirname(__file__), 'setup.py'))],
    # )

    process_pipeline_options = pipeline_options.view_as(options.ProcessPipelineOptions)
    cloud_options = pipeline_options.view_as(GoogleCloudOptions)
    cloud_options.project = _default_project()
    standard_options = pipeline_options.view_as(StandardOptions)
    pipeline_options.view_as(SetupOptions).setup_file = os.path.abspath(os.path.join(os.path.dirname(__file__), 'setup.py'))

    if process_pipeline_options.mode == tf.contrib.learn.ModeKeys.EVAL:

        if process_pipeline_options.random_location == "":
            raise ValueError('random_location not set')

        random_occurrences.fetch_random(
            pipeline_options=pipeline_options,
            output_path=os.path.join(
                process_pipeline_options.random_location,
                datetime.datetime.now().strftime("%s"),
            )
        )

    elif process_pipeline_options.mode == tf.contrib.learn.ModeKeys.TRAIN:

        if process_pipeline_options.intermediate_location == "":
            raise ValueError('intermediate_location not set')

        occurrences.fetch_occurrences(
            pipeline_options=pipeline_options,
            output_path=os.path.join(
                process_pipeline_options.intermediate_location,
                datetime.datetime.now().strftime("%s"),
            )
        )

    elif process_pipeline_options.mode == tf.contrib.learn.ModeKeys.INFER:

        if process_pipeline_options.infer_location == "":
            raise ValueError('infer_location not set')

        protected_areas.fetch_forests(
            pipeline_options=pipeline_options,
            output_path=os.path.join(process_pipeline_options.infer_location, datetime.datetime.now().strftime("%s"))
        )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    main()