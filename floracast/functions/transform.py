from tensorflow_transform.tf_metadata import dataset_schema, dataset_metadata
import tensorflow as tf
from os.path import join, split
import tensorflow_transform as tft
import constants
import apache_beam as beam
from google.cloud import storage
from datetime import datetime
from tensorflow_transform import coders

TRANSFORMED_RAW_METADATA_PATH = "raw_metadata"
TRANSFORMED_METADATA_PATH = "transformed_metadata"
TRANSFORM_FN_PATH = "transform_fn"

class FetchExampleFiles(beam.DoFn):
    def __init__(self, project, bucket):
        super(FetchExampleFiles, self).__init__()
        self._project = project
        self._bucket = bucket

    def process(self, i):

        client = storage.client.Client(project=self._project)
        bucket = client.get_bucket(self._bucket)

        prefix = "gs://"+self._bucket

        files = {}
        for b in bucket.list_blobs(prefix="occurrences", fields="items/name"):
            p, n = split(b.name)
            if not p or not n or p == "/" or n == "/":
                continue
            if p in files:
                files[p] = n if n > files[p] else files[p]
            else:
                files[p] = n

        for b in bucket.list_blobs(prefix="protected_areas", fields="items/name"):
            p, n = split(b.name)
            if not p or not n or p == "/" or n == "/":
                continue
            if p in files:
                files[p] = n if n > files[p] else files[p]
            else:
                files[p] = n

        for i in files:
            yield join(prefix, i, files[i])

        random_paths = []
        randoms = {}
        for b in bucket.list_blobs(prefix="random", fields="items/name"):
            p, n = split(b.name)
            if not p or not n or p == "/" or n == "/":
                continue
            if p in random_paths:
                randoms[p].append(n)
            else:
                random_paths.append(p)
                randoms[p] = [n]

        random_paths = sorted(random_paths, reverse=True)
        if len(random_paths) == 0:
            return

        for f in randoms[random_paths[0]]:
            yield join(prefix, random_paths[0], f)


class TransformData:

    def __init__(self, bucket="floracast-datamining"):

        self.output_path = "gs://" + bucket + "/transformers/" + datetime.now().strftime("%s")

        self.raw_metadata_path = join(self.output_path, TRANSFORMED_RAW_METADATA_PATH)
        self.transformed_metadata_path = join(self.output_path, TRANSFORMED_METADATA_PATH)
        self.transform_fn_path = join(self.output_path, TRANSFORM_FN_PATH)
        self.coder = coders.ExampleProtoCoder(self.create_raw_metadata(tf.estimator.ModeKeys.TRAIN).schema)

    @staticmethod
    def make_preprocessing_fn():

        def preprocessing_fn(i):

            r = {}

            # Identifiable
            # r[constants.KEY_CATEGORY] = tft.string_to_int(i[constants.KEY_CATEGORY])
            r[constants.KEY_CATEGORY] = i[constants.KEY_CATEGORY]
            # Hopefully this will bucket Random and NameUsage without the necessity for boolean conversion.

            # Categorical
            token_list = tf.split(i[constants.KEY_S2_TOKENS], num_or_size_splits=17, axis=1)
            for token_level in range(17):
                r[ 's2_token_%d' % token_level] = tft.string_to_int(token_list[token_level], default_value=0)

            r[constants.KEY_ECO_BIOME] = tft.string_to_int(i[constants.KEY_ECO_BIOME], default_value=0)
            r[constants.KEY_ECO_NUM] = tft.string_to_int(i[constants.KEY_ECO_NUM], default_value=0)

            # Ordinal
            # TODO: Add feature: difference between min and max temp.
            # # And then include average temperature as the other.
            r[constants.KEY_ELEVATION] = tft.scale_to_0_1(i[constants.KEY_ELEVATION])
            r[constants.KEY_MIN_TEMP] = tft.scale_to_0_1(i[constants.KEY_MIN_TEMP])
            r[constants.KEY_MAX_TEMP] = tft.scale_to_0_1(i[constants.KEY_MAX_TEMP])
            r[constants.KEY_DAYLIGHT] = tft.scale_to_0_1(i[constants.KEY_DAYLIGHT])
            r[constants.KEY_PRCP] = tft.scale_to_0_1(i[constants.KEY_PRCP])

            return r

        return preprocessing_fn

    @staticmethod
    def create_raw_metadata(mode):

        """Input schema definition.
        Args:
          mode: tf.contrib.learn.ModeKeys specifying if the schema is being used for
            train/eval or prediction.
        Returns:
          A `Schema` object.
        """
        result = ({} if mode == tf.estimator.ModeKeys.PREDICT else {
            constants.KEY_CATEGORY: tf.FixedLenFeature(shape=[], dtype=tf.string), # Equivalent of KEY_TAXON
            # constants.KEY_EXAMPLE_ID: FixedLenFeature(shape=[], dtype=string),
        })
        result.update({
            constants.KEY_S2_TOKENS: tf.FixedLenFeature(shape=[17], dtype=tf.string),
            constants.KEY_ECO_BIOME: tf.FixedLenFeature(shape=[], dtype=tf.string),
            constants.KEY_ECO_NUM: tf.FixedLenFeature(shape=[], dtype=tf.string),
            constants.KEY_ELEVATION: tf.FixedLenFeature(shape=[], dtype=tf.int64),
            constants.KEY_MAX_TEMP: tf.FixedLenFeature(shape=[120], dtype=tf.float32),
            constants.KEY_MIN_TEMP: tf.FixedLenFeature(shape=[120], dtype=tf.float32),
            constants.KEY_AVG_TEMP: tf.FixedLenFeature(shape=[120], dtype=tf.float32),
            constants.KEY_PRCP: tf.FixedLenFeature(shape=[120], dtype=tf.float32),
            constants.KEY_DAYLIGHT: tf.FixedLenFeature(shape=[120], dtype=tf.float32),
        })

        return dataset_metadata.DatasetMetadata(schema=dataset_schema.from_feature_spec(result))


    # features['elevation'].set_shape((1,))
    # features['label'].set_shape([1,1])

    # features['tmax'].set_shape([FLAGS.days_before_occurrence,1])
    # features['tmin'].set_shape([FLAGS.days_before_occurrence,1])
    # features['prcp'].set_shape([FLAGS.days_before_occurrence,1])
    # features['daylight'].set_shape([FLAGS.days_before_occurrence,1])
    #
    # # features['tmaxstacked'] = tf.reshape(features['tmax'], [9, 5])
    # tmax = tf.reduce_mean(tf.reshape(features['tmax'], [9, 5]), 1)
    # prcp = tf.reduce_mean(tf.reshape(features['prcp'], [9, 5]), 1)
    # daylight = tf.reduce_mean(tf.reshape(features['daylight'], [9, 5]), 1)
    #
    # x = tf.concat([tmax, prcp, daylight, features['elevation']], 0)