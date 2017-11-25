# Copyright 2017 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#            http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Sample for Reddit dataset can be run as a wide or deep model."""

# from __future__ import absolute_import
from __future__ import division
from __future__ import print_function


def create_parser():
    import argparse
    """Initialize command line parser using arparse.
    Returns:
      An argparse.ArgumentParser.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--transformed_path', type=str, required=True)
    parser.add_argument('--output_path', type=str, required=True)
    # parser.add_argument(
    #     '--hidden_units',
    #     nargs='*',
    #     help='List of hidden units per layer. All layers are fully connected. Ex.'
    #          '`64 32` means first layer has 64 nodes and second one has 32.',
    #     # default=[64, 32],
    #     type=int)
    # parser.add_argument(
    #     '--batch_size',
    #     help='Number of input records used per batch',
    #     default=100,
    #     type=int)
    # parser.add_argument(
    #     '--eval_batch_size',
    #     help='Number of eval records used per batch',
    #     default=5000,
    #     type=int)
    # parser.add_argument(
    #     '--train_steps', help='Number of training steps to perform.', type=int)
    # parser.add_argument(
    #     '--eval_steps',
    #     help='Number of evaluation steps to perform.',
    #     type=int,
    #     default=100)
    # parser.add_argument(
    #     '--train_set_size',
    #     help='Number of samples on the train dataset.',
    #     type=int,
    #     required=True)
    # parser.add_argument(
    #     '--num_epochs', help='Number of epochs', default=100, type=int)

    # parser.add_argument(
    #     '--num_classes', help='Number of classes', required=True, type=int)
    return parser

#
# def get_experiment_fn(run_config, args):
#     """Wrap the get experiment function to provide the runtime arguments."""
#
#     # def get_experiment(run_config, params):
#     import tensorflow as tf
#     from tensorflow_transform.tf_metadata import metadata_io
#     from tensorflow_transform.saved import input_fn_maker
#     import os
#     import model, input_fn
#
#     """Function that creates an experiment http://goo.gl/HcKHlT.
#     Args:
#       output_dir: The directory where the training output should be written.
#     Returns:
#       A `tf.contrib.learn.Experiment`.
#     """
#
#     # min_eval_frequency=500
#
#     cluster = run_config.cluster_spec
#     num_table_shards = max(1, run_config.num_ps_replicas * 3)
#     num_partitions = max(1, 1 + cluster.num_tasks('worker') if cluster and 'worker' in cluster.jobs else 0)
#
#     classifier = model.get_estimator(args=args, run_config=run_config)
#
#     serving_input_fn = input_fn_maker.build_parsing_transforming_serving_input_receiver_fn(
#         raw_metadata=metadata_io.read_metadata(os.path.join(args.train_data_path, "raw_metadata")),
#         transform_savedmodel_dir=os.path.join(args.train_data_path, "transform_fn"),
#         exclude_raw_keys=['taxon']
#     )
#
#     # serving_input_fn = input_fn.get_serving_input_fn(
#     #         args=args,
#     #         raw_label_keys=['taxon'])
#
#     export_strategy = tf.contrib.learn.utils.make_export_strategy(
#         serving_input_fn,
#         exports_to_keep=1,
#     )
#
#     transformed_metadata = metadata_io.read_metadata(
#         os.path.join(args.train_data_path, "transformed_metadata"))
#
#     train_input_fn = input_fn.get_transformed_reader_input_fn(
#         transformed_metadata,
#         args.train_data_path + "/train_data/*.gz",
#         args.batch_size,
#         tf.estimator.ModeKeys.TRAIN)
#
#     eval_input_fn = input_fn.get_transformed_reader_input_fn(
#         transformed_metadata,
#         args.train_data_path + "/eval_data/*.gz",
#         args.batch_size,
#         tf.estimator.ModeKeys.EVAL)
#
#     return tf.contrib.learn.Experiment(
#         estimator=classifier,
#         train_steps=(args.num_epochs * args.train_set_size // args.batch_size),
#         eval_steps=args.eval_steps,
#         train_input_fn=train_input_fn,
#         eval_input_fn=eval_input_fn,
#         export_strategies=export_strategy)
#
#     # Return a function to create an Experiment.
#     # return get_experiment


def main(argv=None):
    import sys
    import json
    from datetime import datetime
    import tensorflow as tf
    from tensorflow_transform.tf_metadata import metadata_io
    from tensorflow_transform.saved import input_fn_maker
    import os
    from train_shared import model, input_fn

    """Run a Tensorflow model on the Reddit dataset."""
    env = json.loads(os.environ.get('TF_CONFIG', '{}'))
    # First find out if there's a task value on the environment variable.
    # If there is none or it is empty define a default one.
    task_data = env.get('task') or {'type': 'master', 'index': 0}
    argv = sys.argv if argv is None else argv
    args = create_parser().parse_args(args=argv[1:])

    output_path = os.path.join(args.output_path, datetime.now().strftime("%s"))

    trial = task_data.get('trial')
    if trial is not None:
        output_dir = os.path.join(output_path, trial)
    else:
        output_dir = output_path

    run_config = tf.estimator.RunConfig(
        model_dir=output_dir
    )
    # run_config = tf.contrib.learn.RunConfig()
    # run_config = run_config.replace(model_dir=output_dir)

    # min_eval_frequency=500

    # cluster = run_config.cluster_spec
    # num_table_shards = max(1, run_config.num_ps_replicas * 3)
    # num_partitions = max(1, 1 + cluster.num_tasks('worker') if cluster and 'worker' in cluster.jobs else 0)

    classifier = model.get_estimator(args=args, run_config=run_config)

    serving_input_fn = input_fn_maker.build_parsing_transforming_serving_input_receiver_fn(
        raw_metadata=metadata_io.read_metadata(os.path.join(args.transformed_path, "raw_metadata")),
        transform_savedmodel_dir=os.path.join(args.transformed_path, "transform_fn"),
        exclude_raw_keys=['taxon']
    )

    # serving_input_fn = input_fn.get_serving_input_fn(
    #         args=args,
    #         raw_label_keys=['taxon'])

    # export_strategy = tf.contrib.learn.utils.make_export_strategy(
    #     serving_input_fn,
    #     exports_to_keep=1,
    # )

    transformed_metadata = metadata_io.read_metadata(
        os.path.join(args.transformed_path, "transformed_metadata"))

    train_input_fn = input_fn.get_transformed_reader_input_fn(
        transformed_metadata,
        args.transformed_path + "/train_data/*.gz",
        20,
        # args.batch_size,
        tf.estimator.ModeKeys.TRAIN)

    eval_input_fn = input_fn.get_transformed_reader_input_fn(
        transformed_metadata,
        args.transformed_path + "/eval_data/*.gz",
        20,
        # args.batch_size,
        tf.estimator.ModeKeys.EVAL)

    # classifier.train(input_fn=train_input_fn, max_steps=1000)
    #
    # metrics = classifier.evaluate(eval_input_fn)
    #
    # print(metrics)
    #
    # return

    # return tf.contrib.learn.Experiment(
    #     estimator=classifier,
    #     train_steps=(args.num_epochs * args.train_set_size // args.batch_size),
    #     eval_steps=args.eval_steps,
    #     train_input_fn=train_input_fn,
    #     eval_input_fn=eval_input_fn,
    #     export_strategies=export_strategy)

    train_spec = tf.estimator.TrainSpec(input_fn=train_input_fn, max_steps=1000)
    eval_spec = tf.estimator.EvalSpec(
        input_fn=eval_input_fn,
        exporters=tf.estimator.FinalExporter("exporter", serving_input_fn)
    )

    res = tf.estimator.train_and_evaluate(classifier, train_spec, eval_spec)
    print(res)

    # eval, export = experiment.train_and_evaluate()

    # print(eval)
    # print(export)

    return

    # """Wrap the get input features function to provide the runtime arguments."""
    #
    # for p in experiment.estimator.predict(
    #     input_fn=input_fn.get_test_prediction_data_fn(args),
    #     # as_iterable=False
    # ):
    #     print(p)
    #
    # return
    #
    # if len(export) == 0:
    #     return
    #
    # dirs = export[0].split('/')
    # n = len(dirs) - 4
    #
    # local_model_path = '/'.join(dirs[:len(dirs) - 3])
    #
    # print("gsutil cp -r %s gs://floracast-models/models/" % local_model_path)
    #
    # gs_model_path = ("gs://floracast-models/models/%s" % '/'.join(dirs[n:]))
    #
    # version = randint(0, 100)
    # print("gcloud ml-engine versions create 'v%d' \
    #     --model 'occurrences' \
    #     --origin %s" % (version, gs_model_path))
    #
    # job_id = randint(0, 1000000)
    # print(("gcloud ml-engine jobs submit prediction 'occurrences_%d' \
    #     --version 'v%d' \
    #     --model 'occurrences' \
    #     --input-paths gs://floracast-models/forests/data.tfrecords \
    #     --output-path gs://floracast-models/predictions/%d/ \
    #     --region us-east1 \
    #     --data-format TF_RECORD") % (job_id, version, job_id))


    # run_config = run_config.replace(save_checkpoints_steps=params.min_eval_frequency)
    # learn_runner.run(experiment_fn=get_experiment_fn(args),
    #                  run_config=run_config)




if __name__ == '__main__':
    main()