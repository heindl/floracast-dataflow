import apache_beam as beam

KEY_OCCURRENCE_ID = 'occurrence_id'
KEY_TAXON = 'taxon'
KEY_LATITUDE = 'latitude'
KEY_LONGITUDE = 'longitude'
KEY_ELEVATION = 'elevation'
KEY_DATE ='date'
KEY_AVG_TEMP = 'avg_temp'
KEY_MAX_TEMP = 'max_temp'
KEY_MIN_TEMP = 'min_temp'
KEY_PRCP = 'precipitation'
KEY_DAYLIGHT = 'daylight'
KEY_GRID_ZONE = 'mgrs_grid_zone'

@beam.ptransform_fn
def Shuffle(pcoll):  # pylint: disable=invalid-name
    import random
    return (pcoll
            | 'PairWithRandom' >> beam.Map(lambda x: (random.random(), x))
            | 'GroupByRandom' >> beam.GroupByKey()
            | 'DropRandom' >> beam.FlatMap(lambda (k, vs): vs))

# def partition_fn(user_id, partition_random_seed, percent_eval):
# I hope taxon_id will provide wide enough variation between results.
def partition_fn(ex, partition_random_seed, percent_eval):
    import hashlib
    m = hashlib.md5(str(ex["occurrence_id"]) + str(partition_random_seed))
    hash_value = int(m.hexdigest(), 16) % 100
    return 0 if hash_value >= percent_eval else 1

def make_preprocessing_fn(num_classes):
    import tensorflow_transform as tt
    """Creates a preprocessing function for reddit.
    Args:

    Returns:
      A preprocessing function.
    """

    def preprocessing_fn(i):

        # import tensorflow as tf

        m = {
            KEY_OCCURRENCE_ID: i[KEY_OCCURRENCE_ID],
            KEY_ELEVATION: tt.scale_to_0_1(i[KEY_ELEVATION]),
            KEY_AVG_TEMP: tt.scale_to_0_1(i[KEY_AVG_TEMP]),
            KEY_MIN_TEMP: tt.scale_to_0_1(i[KEY_MIN_TEMP]),
            KEY_MAX_TEMP: tt.scale_to_0_1(i[KEY_MAX_TEMP]),
            KEY_PRCP: tt.scale_to_0_1(i[KEY_PRCP]),
            KEY_DAYLIGHT: tt.scale_to_0_1(i[KEY_DAYLIGHT]),
            # KEY_GRID_ZONE: tt.hash_strings(i[KEY_GRID_ZONE], 1000)
            KEY_GRID_ZONE: i[KEY_GRID_ZONE],
            KEY_TAXON: i[KEY_TAXON]
        }

        # def relable_fn(v):
        #     print("called", v[0])
        #     if v == 0:
        #         return [0]
        #     else:
        #         return [1]

        # m[KEY_TAXON] = tf.cast(i[KEY_TAXON], tf.string)

        # if num_classes == 2:
        #     # m[KEY_TAXON] = tt.apply_function(relable_fn, i[KEY_TAXON])
        #     m[KEY_TAXON] = tf.cast(i[KEY_TAXON], tf.bool)
        #     m[KEY_TAXON] = tf.cast(m[KEY_TAXON], tf.int64)
        # else:
        #     m[KEY_TAXON] = i[KEY_TAXON]

        return m
        # m = {}
        # m[KEY_ELEVATION] = tt.scale_to_0_1(inputs[KEY_ELEVATION])
        # m[KEY_MAX_TEMP] = tt.scale_to_0_1(inputs[KEY_MAX_TEMP])
        # m[KEY_MIN_TEMP] = tt.scale_to_0_1(inputs[KEY_MIN_TEMP])
        # m[KEY_AVG_TEMP] = tt.scale_to_0_1(inputs[KEY_AVG_TEMP])
        # m[KEY_PRCP] = tt.scale_to_0_1(inputs[KEY_PRCP])
        # m[KEY_DAYLIGHT] = tt.scale_to_0_1(inputs[KEY_DAYLIGHT])
        #
        # m[KEY_GRID_ZONE] = tt.hash_strings(inputs[KEY_GRID_ZONE], 8)

        # m['tmax'] = array_ops.reshape(m['tmax'])

        # return m

    return preprocessing_fn

def make_input_schema(mode):
    from tensorflow_transform.tf_metadata import dataset_schema
    from tensorflow import FixedLenFeature, float32, string, int64, VarLenFeature
    from tensorflow.contrib.learn import ModeKeys
    """Input schema definition.
    Args:
      mode: tf.contrib.learn.ModeKeys specifying if the schema is being used for
        train/eval or prediction.
    Returns:
      A `Schema` object.
    """
    result = ({} if mode == ModeKeys.INFER else {
        KEY_TAXON: FixedLenFeature(shape=[], dtype=string)
    })
    result.update({
        KEY_OCCURRENCE_ID: FixedLenFeature(shape=[], dtype=string),
        KEY_ELEVATION: FixedLenFeature(shape=[1], dtype=float32),
        KEY_GRID_ZONE: FixedLenFeature(shape=[1], dtype=string),
        KEY_MAX_TEMP: FixedLenFeature(shape=[45], dtype=float32),
        KEY_MIN_TEMP: FixedLenFeature(shape=[45], dtype=float32),
        KEY_AVG_TEMP: FixedLenFeature(shape=[45], dtype=float32),
        KEY_PRCP: FixedLenFeature(shape=[45], dtype=float32),
        KEY_DAYLIGHT: FixedLenFeature(shape=[45], dtype=float32),
    })

    return dataset_schema.from_feature_spec(result)


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
