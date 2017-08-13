import tensorflow as tf
import constants

def create_feature_columns():

    # Context columns

    # label = tf.contrib.layers.sparse_column_with_hash_bucket(
    #     column_name="label",
    #     hash_bucket_size=1000
    # )
    # Note that in the example label is a real valued column. Why would this be?
    # In the example, it is binary for the income bracket. Since it is a categorization task rather,
    # i think it would work and would avoid an embedding column.
    label = tf.contrib.layers.real_valued_column("label", dtype=tf.int64)
    # Grid zone could potentially be a sparse_column_with_keys
    grid_zone = tf.contrib.layers.sparse_column_with_hash_bucket(
        column_name="grid-zone",
        hash_bucket_size=1000
    )
    elevation = tf.contrib.layers.real_valued_column("elevation", dtype=tf.float32, dimension=1)

    # Sequence Columns
    tmax = tf.contrib.layers.real_valued_column("tmax", dtype=tf.float32, dimension=constants.WeatherDaysBeforeOccurrence)
    tmin = tf.contrib.layers.real_valued_column("tmin", dtype=tf.float32, dimension=constants.WeatherDaysBeforeOccurrence)
    prcp = tf.contrib.layers.real_valued_column("prcp", dtype=tf.float32, dimension=constants.WeatherDaysBeforeOccurrence)
    daylight = tf.contrib.layers.real_valued_column("daylight", dtype=tf.float32, dimension=constants.WeatherDaysBeforeOccurrence)

    return set([
        label,
        grid_zone,
        elevation,
        tmax,
        tmin,
        prcp,
        daylight,
    ])

def input_fn(mode, data_file, batch_size):
    input_features = create_feature_columns()
    features = tf.contrib.layers.create_feature_spec_for_parsing(input_features)

    feature_map = tf.contrib.learn.io.read_batch_record_features(
        file_pattern=[data_file],
        batch_size=batch_size,
        features=features,
        name="read_batch_features_{}".format(mode))

    target = feature_map.pop("label")

    return feature_map, target