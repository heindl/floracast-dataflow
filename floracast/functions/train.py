import tensorflow as tf
from os.path import isdir, join
from os import makedirs, walk
from tensorflow_transform.tf_metadata import metadata_io
from tensorflow_transform.saved import input_fn_maker, saved_transform_io
from tensorflow.contrib.learn.python.learn.utils import input_fn_utils
import constants
from math import ceil
import functools
import six
from datetime import datetime
import transform
from google.cloud import storage
import string
import random
import errno
from tensorflow.python.lib.io import tf_record
TFRecordCompressionType = tf_record.TFRecordCompressionType


class TrainingData:

    _TEMP_DIR = "/tmp/"

    def __init__(self,
                 project,
                 gcs_bucket,
                 name_usage_id,
                 train_batch_size,
                 train_epochs,
                 occurrence_records,
                 transform_data_path=None,
                 model_path=None,
                 experiment=None
                 ):

        self._experiment = experiment
        if self._experiment is not None:
            print('Launching Experiment %d' % self._experiment['id'])

        self._name_usage_id = name_usage_id
        self._train_batch_size = train_batch_size
        self._train_epochs = train_epochs
        self._ts = datetime.now().strftime("%s")
        self._project=project
        self._gcs_bucket=gcs_bucket

        self._local_path = self._make_temp_dir()

        self._model_path = model_path if model_path else join(self._local_path, "model")

        self._transform_data_path = transform_data_path if transform_data_path else self._fetch_latest_transformer()

        self._raw_metadata_path = join(self._transform_data_path, transform.TRANSFORMED_RAW_METADATA_PATH)
        self._transformed_metadata_path = join(self._transform_data_path, transform.TRANSFORMED_METADATA_PATH)
        self._transform_fn_path = join(self._transform_data_path, transform.TRANSFORM_FN_PATH)

        for dir in [
            self._raw_metadata_path,
            self._transformed_metadata_path,
            self._transform_fn_path,
        ]:
            if not isdir(dir):
                raise ValueError("Directory doesn't exist: ", dir)

        self._tfrecord_parser = occurrence_records

    def __del__(self):
        # cleanup local occurrence data.
        # if not self._local_path.startswith(self._TEMP_DIR):
        #     raise ValueError("Invalid Train Output Path")
        # if isdir(self._local_path):
        #     rmtree(self._local_path)
        # print("local_path", self._local_path)
        # print("model_path", self._model_path)
        print("transform_path", self._transform_data_path)


    def _make_temp_dir(self):
        dir = self._TEMP_DIR + "".join(random.choice(string.ascii_letters + string.digits) for x in range(random.randint(8, 12)))
        makedirs(dir)
        return dir

    def _fetch_latest_transformer(self):
        bucket = storage.Client(project=self._project).bucket(self._gcs_bucket)

        file_list = {}
        for b in bucket.list_blobs(prefix="transformers/"):
            if b.name.endswith("/"):
                dirname = "/".join(b.name.split("/")[2:])
                if len(dirname) > 0:
                    try:
                        makedirs(join(self._local_path, dirname))
                    except OSError as err:
                        if err == errno.EEXIST:
                            pass
                continue
            d = b.name.split("/")[1]
            if d in file_list:
                file_list[d].append(b)
            else:
                file_list[d] = [b]

        latest_date = sorted(list(file_list), reverse=True)[0]
        #
        # bucket.get_blob("transformers/"+latest_date+"/").download_to_filename(self._local_path)

        for f in file_list[latest_date]:
            fname = "/".join(f.name.split("/")[2:])
            f.download_to_filename(join(self._local_path, fname))

        return join(self._local_path)

    def _feature_columns(self):

        meta_data = metadata_io.read_metadata(self._transformed_metadata_path)

        res = []

        # Note: Tested ECO_BIOME & ECO_Region individually, and as seperate features in the model,
        # but the crossed column has a slightly higher precision.
        if constants.KEY_ECO_REGION in self._experiment['columns']:
            eco_num_buckets = meta_data.schema[constants.KEY_ECO_NUM].domain.max_value
            eco_biome_buckets = meta_data.schema[constants.KEY_ECO_BIOME].domain.max_value
            eco_biome_column = tf.feature_column.categorical_column_with_identity(
                constants.KEY_ECO_BIOME,
                num_buckets=eco_biome_buckets,
                default_value=0
            )
            eco_num_column = tf.feature_column.categorical_column_with_identity(
                constants.KEY_ECO_NUM,
                num_buckets=eco_num_buckets,
                default_value=0
            )
            region_column = tf.feature_column.crossed_column(
                [eco_biome_column, eco_num_column],
                hash_bucket_size=1000
            )
            # Note: Tried embedding_column but indicator column was significantly more accurate.
            res.append(tf.feature_column.indicator_column(region_column))

        for column in self._experiment['columns']:
            if 's2_token_' in column:
                s2_token_buckets = meta_data.schema[column].domain.max_value
                s2_token_column = tf.feature_column.categorical_column_with_identity(
                    column,
                    num_buckets=s2_token_buckets,
                    default_value=0
                )
                s2_token_embedding_dimensions = ceil(s2_token_buckets ** 0.25)
                res.append(tf.feature_column.embedding_column(s2_token_column, dimension=s2_token_embedding_dimensions))

        if constants.KEY_MAX_TEMP in self._experiment['columns']:
            res.append(tf.feature_column.numeric_column(
                constants.KEY_MAX_TEMP,
                shape=[self._experiment['shape']])
            )

        if constants.KEY_MIN_TEMP in self._experiment['columns']:
            res.append(tf.feature_column.numeric_column(
                constants.KEY_MIN_TEMP,
                shape=[self._experiment['shape']])
            )

        if constants.KEY_PRCP in self._experiment['columns']:
            res.append(tf.feature_column.numeric_column(
                constants.KEY_PRCP,
                shape=[self._experiment['shape']])
            )

        if constants.KEY_ELEVATION in self._experiment['columns']:
            res.append(tf.feature_column.numeric_column(constants.KEY_ELEVATION, shape=[]))

        if constants.KEY_DAYLIGHT in self._experiment['columns']:
            res.append(tf.feature_column.numeric_column(constants.KEY_DAYLIGHT, shape=[self._experiment['shape']]))

        return res


    def _reshape_weather(self, f):
        days = 2
        if self._experiment is not None:
            days = self._experiment['days']

        f = tf.reshape(f, [120 / days, days])
        f = tf.reduce_mean(f, 1)
        f = tf.slice(f, [self._experiment['slice_index']], [self._experiment['shape']])
        return f

    def _all_feature_keys(self):
        return self._weather_keys() + [
            constants.KEY_ELEVATION,
            constants.KEY_ECO_NUM,
            constants.KEY_ECO_BIOME,
            constants.KEY_S2_TOKENS,
            # Maybe 's2_token_3'
            's2_token_3',
            's2_token_4',
            's2_token_5',
        ]

    def _weather_keys(self):
        return [
            constants.KEY_MAX_TEMP,
            constants.KEY_MIN_TEMP,
            constants.KEY_PRCP,
            constants.KEY_DAYLIGHT,
        ]

    # def get_label_vocabularly(train_data_path):
    #     labels = []
    #     label_files = glob.glob(train_data_path + "/labels*")
    #     for file in label_files:
    #         with open(file, 'r') as label_file:
    #             taxa = label_file.read().splitlines()
    #             for t in taxa:
    #                 labels.append(t)
    #
    #     return labels

    def _gzip_reader_fn(self):
        return tf.TFRecordReader(
            options=tf.python_io.TFRecordOptions(
                compression_type=tf.python_io.TFRecordCompressionType.GZIP,
            )
        )

    def input_functions(self, percentage_split):

        eval_file, train_file = self._tfrecord_parser.train_test_split(percentage_split)

        eval_batch_size = self._tfrecord_parser.count(eval_file, compression_type=TFRecordCompressionType.GZIP)

        train_fn = functools.partial(self._transformed_input_fn,
                                     raw_data_file_pattern=train_file,
                                     mode=tf.estimator.ModeKeys.TRAIN,
                                     batch_size=self._train_batch_size,
                                     epochs=self._train_epochs)

        eval_fn = functools.partial(self._transformed_input_fn,
                                    raw_data_file_pattern=eval_file,
                                    mode=tf.estimator.ModeKeys.EVAL,
                                    batch_size=eval_batch_size,
                                    epochs=1)

        return eval_fn, train_fn

    def _raw_metadata(self):
        return metadata_io.read_metadata(self._raw_metadata_path)

    def _transformed_metadata(self):
        return metadata_io.read_metadata(self._transformed_metadata_path)

    def _transformed_input_fn(self, raw_data_file_pattern, mode, batch_size, epochs):

        labels = ([constants.KEY_EXAMPLE_ID] if mode == tf.estimator.ModeKeys.PREDICT else [constants.KEY_CATEGORY])

        fn = input_fn_maker.build_transforming_training_input_fn(
            raw_metadata=self._raw_metadata(),
            transformed_metadata=self._transformed_metadata(),
            transform_savedmodel_dir=self._transform_fn_path,
            raw_data_file_pattern=raw_data_file_pattern,
            training_batch_size=batch_size,
            # transformed_label_keys=constants.KEY_CATEGORY,
            transformed_label_keys=labels,
            transformed_feature_keys=self._all_feature_keys(),
            key_feature_name=None,
            convert_scalars_to_vectors=True,
            # Read batch features.
            reader=self._gzip_reader_fn,
            # num_epochs=(1 if mode != estimator.ModeKeys.TRAIN else None),
            num_epochs=epochs,
            randomize_input=True,
            # randomize_input=(mode == estimator.ModeKeys.TRAIN),
            queue_capacity=(batch_size + 1) if (mode == tf.estimator.ModeKeys.EVAL) else (batch_size * 20),
        )

        features, labels = fn()
        labels = tf.reshape(labels, [-1])
        labels = tf.not_equal(labels, "random")

        # if mode == tf.estimator.ModeKeys.EVAL:
        #     labels = tf.Print(labels, [labels])

        for weather_key in self._weather_keys():
            if weather_key in self._experiment['columns']:
                features[weather_key] = tf.map_fn(self._reshape_weather, features[weather_key])

        if mode == tf.estimator.ModeKeys.PREDICT:
            # features[constants.KEY_OCCURRENCE_ID] = labels
            return features
        else:
            return features, labels

    def _convert_scalars_to_vectors(self, features):
        """Vectorize scalar columns to meet FeatureColumns input requirements."""
        def maybe_expand_dims(tensor):
            # Ignore the SparseTensor case.  In principle it's possible to have a
            # rank-1 SparseTensor that needs to be expanded, but this is very
            # unlikely.
            if isinstance(tensor, tf.Tensor) and tensor.get_shape().ndims == 1:
                tensor = tf.expand_dims(tensor, -1)
            return tensor

        return {name: maybe_expand_dims(tensor)
                for name, tensor in six.iteritems(features)}

    def _serving_input_receiver_fn(self):

        raw_feature_spec = self._raw_metadata().schema.as_feature_spec()

        # Exclude label keys and other unnecessary features.
        # This is typically used to specify the raw labels and weights,
        # so that transformations involving these do not pollute the serving graph.
        all_raw_feature_keys = six.iterkeys(self._raw_metadata().schema.column_schemas)
        raw_feature_keys = list(set(all_raw_feature_keys) - set(constants.KEY_CATEGORY))

        raw_serving_feature_spec = {key: raw_feature_spec[key]
                                    for key in raw_feature_keys}

        def parsing_transforming_serving_input_receiver_fn():
            """Serving input_fn that applies transforms to raw data in tf.Examples."""
            raw_input_fn = input_fn_utils.build_parsing_serving_input_fn(
                raw_serving_feature_spec, default_batch_size=None)
            raw_features, _, inputs = raw_input_fn()
            _, transformed_features = (
                saved_transform_io.partially_apply_saved_transform(
                    self._transform_fn_path, raw_features))

            # Convert scalars to vectors
            transformed_features = self._convert_scalars_to_vectors(transformed_features)

            for key in self._weather_keys():
                transformed_features[key] = tf.map_fn(self._reshape_weather, transformed_features[key])

            return tf.estimator.export.ServingInputReceiver(
                transformed_features, inputs)

        return parsing_transforming_serving_input_receiver_fn

    def export_model(self):
        model = self.get_estimator()
        model.export_savedmodel(
            export_dir_base=join(self._model_path, "exports/"),
            serving_input_receiver_fn=self._serving_input_receiver_fn()
        )

    def upload_exported_model(self):
        bucket = storage.Client(project=self._project).bucket(self._gcs_bucket)

        export_dir = join(self._model_path, "exports/")
        for subdir, dirs, files in walk(export_dir):
            subdir = subdir[len(export_dir):]
            for file in files:
                local_path = join(export_dir, subdir, file)
                gcs_path = "models/%s/%s" % (self._name_usage_id, join(subdir, file))
                bucket.blob(gcs_path).upload_from_filename(local_path)


    def get_estimator(self):

        # run_config = tf.estimator.RunConfig(model_dir=FLAGS.model_dir)

        return tf.estimator.DNNClassifier(
            feature_columns=self._feature_columns(),
            hidden_units=[100, 75, 50, 25],
            # hidden_units=[75],
            # optimizer=tf.train.ProximalAdagradOptimizer(
            #     learning_rate=0.01,
            #     l1_regularization_strength=0.001
            # ),
            model_dir=self._model_path,
        )

# def get_estimator(run_config, feature_columns):
#
#     def _get_model_fn(estimator):
#         # def _model_fn(features, labels, mode):
#         def _model_fn(features, labels, mode, config):
#             if mode == tf.estimator.ModeKeys.PREDICT:
#                 key = features.pop(constants.KEY_EXAMPLE_ID)
#             # params = estimator.params
#             model_fn_ops = estimator._model_fn(
#                 # features=features, labels=labels, mode=mode, params=params)
#                 features=features, labels=labels, mode=mode, config=config)
#             if mode == tf.estimator.ModeKeys.PREDICT:
#                 model_fn_ops.predictions[constants.KEY_EXAMPLE_ID] = key
#                 # model_fn_ops.output_alternatives[None][1]['occurrence_id'] = key
#             return model_fn_ops
#         return _model_fn
#
#         # classifier = tf.contrib.learn.Estimator(
#     return tf.estimator.Estimator(
#         model_fn=_get_model_fn(
#             # tf.contrib.learn.DNNClassifier(
#             tf.estimator.DNNClassifier(
#                 feature_columns=feature_columns,
#                 hidden_units=[100, 75, 50, 25],
#                 # optimizer=tf.train.ProximalAdagradOptimizer(
#                 #     learning_rate=0.01,
#                 #     l1_regularization_strength=0.001
#                 # ),
#             )
#         ),
#         config=run_config,
#     )