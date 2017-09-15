# from __future__ import absolute_import
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


class ExampleCoder(beam.coders.Coder):

    def __init__(self):
        super(ExampleCoder, self).__init__()

    def encode(self, example):
        return example.encode()

    def decode(self, s):
        return Example().decode_from_string(s)

    def is_deterministic(self):
        return True

# LIST_TYPES
LIST_TYPE_INT64 = 'int64'
LIST_TYPE_FLOAT = 'float'
LIST_TYPE_BYTES = 'bytes'

class Example:

    def __init__(self, example=None):
        from tensorflow.core.example import example_pb2
        if example is not None:
            self._example = example
        else:
            self._example = example_pb2.Example()

    def _append_value(self, feature, typer, value):
        if typer == LIST_TYPE_INT64:
            self._example.features.feature[feature].int64_list.value.append(value)
        elif typer == LIST_TYPE_FLOAT:
            self._example.features.feature[feature].float_list.value.append(value)
        elif typer == LIST_TYPE_BYTES:
            self._example.features.feature[feature].bytes_list.value.append(value)

    def as_map(self):
        v = {}
        for k in self._example.features.feature:
            if len(self._example.features.feature[k].int64_list.value) > 0:
                v[k] = self._example.features.feature[k].int64_list.value
                continue
            if len(self._example.features.feature[k].float_list.value) > 0:
                v[k] = self._example.features.feature[k].float_list.value
                continue
            if len(self._example.features.feature[k].bytes_list.value) > 0:
                v[k] = self._example.features.feature[k].bytes_list.value
                continue
        return v

    def _set_value(self, feature, typer, value, i=0):
        if typer == LIST_TYPE_INT64:
            if len(self._example.features.feature[feature].int64_list.value) == 0:
                self._append_value(feature, typer, value)
            else:
                self._example.features.feature[feature].int64_list.value[i] = value
        elif typer == LIST_TYPE_FLOAT:
            if len(self._example.features.feature[feature].float_list.value) == 0:
                self._append_value(feature, typer, value)
            else:
                self._example.features.feature[feature].float_list.value[i] = value
        elif typer == LIST_TYPE_BYTES:
            if len(self._example.features.feature[feature].bytes_list.value) == 0:
                self._append_value(feature, typer, value)
            else:
                self._example.features.feature[feature].bytes_list.value[i] = value

    def _get_value(self, feature, typer):
        v = self._get_values(feature, typer)
        if v is not None:
            return v[0]
        return None

    def _get_values(self, feature, typer):

        f = self._example.features.feature[feature]

        v = []
        if typer == LIST_TYPE_INT64:
            v = f.int64_list.value
        elif typer == LIST_TYPE_FLOAT:
            v = f.float_list.value
        elif typer == LIST_TYPE_BYTES:
            v = f.bytes_list.value
        else:
            return None

        if len(v) == 0:
            return None

        return v

    def occurrence_id(self):
        return self._get_value(KEY_OCCURRENCE_ID, LIST_TYPE_BYTES)

    def set_occurrence_id(self, occurrence_id):
        self._set_value(KEY_OCCURRENCE_ID, LIST_TYPE_BYTES, occurrence_id)

    def taxon(self):
        t = self._get_value(KEY_TAXON, LIST_TYPE_INT64)
        if t is None:
            return 0
        return t

    def set_taxon(self, taxon):
        self._set_value(KEY_TAXON, LIST_TYPE_INT64, taxon)

    def latitude(self):
        return self._get_value(KEY_LATITUDE, LIST_TYPE_FLOAT)

    def set_latitude(self, lat):
        self._set_value(KEY_LATITUDE, LIST_TYPE_FLOAT, lat)
        self._set_grid_zone()

    def longitude(self):
        return self._get_value(KEY_LONGITUDE, LIST_TYPE_FLOAT)

    def set_longitude(self, lng):
        self._set_value(KEY_LONGITUDE, LIST_TYPE_FLOAT, lng)
        self._set_grid_zone()

    def _set_grid_zone(self):
        import mgrs
        lat = self.latitude()
        lng = self.longitude()
        if lat is not None and lng is not None:
            self._set_value(KEY_GRID_ZONE, LIST_TYPE_BYTES, mgrs.MGRS().toMGRS(lat, lng)[:2].encode())

    def date(self):
        return self._get_value(KEY_DATE, LIST_TYPE_INT64)

    def date_string(self):
        from datetime import datetime
        d = datetime.fromtimestamp(self.date())
        return "%d %d %d" % d.year, d.month, d.day


    def set_date(self, date):
        self._set_value(KEY_DATE, LIST_TYPE_INT64, date)

    def coordinates(self):
        return self.latitude(), self.longitude()

    def elevation(self):
        return self._get_value(KEY_ELEVATION, LIST_TYPE_FLOAT)

    def set_elevation(self, elevation):
        self._set_value(KEY_ELEVATION, LIST_TYPE_FLOAT, elevation)

    def as_pb2(self):
        return self._example

    def as_dict(self):
        return self._example.features

    def encode(self):
        return self._example.SerializeToString()

    def decode_from_string(self, s):
        self._example.ParseFromString(s)

    def equality_key(self):
        return "%d|||%.8f|||%.8f|||%d" % (
            self.taxon(),
            self.latitude(),
            self.longitude(),
            self.date()
        )

    def set_random_location_values(self):
        import random
        from datetime import datetime

        NORTHERNMOST = 49.
        SOUTHERNMOST = 25.
        EASTERNMOST = -66.
        WESTERNMOST = -124.

        date = datetime(
            random.randint(2015, 2017),
            random.randint(1, 12),
            random.randint(1, 28)
        )

        self.set_taxon(0)
        self.set_occurrence_id(str(random.randint(100000,900000)))
        self.set_longitude(round(random.uniform(EASTERNMOST, WESTERNMOST), 6))
        self.set_latitude(round(random.uniform(SOUTHERNMOST, NORTHERNMOST), 6))
        self.set_date(int(date.strftime("%s")))
        # while True:
        #     lat = round(random.uniform(self.SOUTHERNMOST, self.NORTHERNMOST), 6)
        #     lng = round(random.uniform(self.EASTERNMOST, self.WESTERNMOST), 6)
        #     try:
        #         gcode = Geocoder.reverse_geocode(lat, lng)
        #
        #         if gcode[0].data[0]['formatted_address'][-6:] in ('Canada', 'Mexico'):
        #             continue
        #         elif 'unnamed road' in gcode[0].data[0]['formatted_address']:
        #             continue
        #         elif 'Unnamed Road' in gcode[0].data[0]['formatted_address']:
        #             continue
        #         else:
        #             return gcode[0].coordinates[0], gcode[0].coordinates[1], date
        #     except GeocoderError:
        #         continue


    def append_temp_avg(self, value):
        self._append_value(KEY_AVG_TEMP, LIST_TYPE_FLOAT, value)

    def append_temp_max(self, value):
        self._append_value(KEY_MAX_TEMP, LIST_TYPE_FLOAT, value)

    def append_temp_min(self, value):
        self._append_value(KEY_MIN_TEMP, LIST_TYPE_FLOAT, value)

    def append_precipitation(self, value):
        self._append_value(KEY_PRCP, LIST_TYPE_FLOAT, value)

    def append_daylight(self, value):
        self._append_value(KEY_DAYLIGHT, LIST_TYPE_FLOAT, value)


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
        KEY_TAXON: FixedLenFeature(shape=[1], dtype=int64)
    })
    result.update({
        KEY_OCCURRENCE_ID: FixedLenFeature(shape=[1], dtype=string),
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


def RandomExample():
    e = Example()
    e.set_random_location_values()
    return e


def FromSerialized(serialized):
    e = Example()
    e.decode_from_string(serialized)
    return e


def make_preprocessing_fn(num_classes):
    import tensorflow_transform as tt
    """Creates a preprocessing function for reddit.
    Args:

    Returns:
      A preprocessing function.
    """

    def preprocessing_fn(i):

        import tensorflow as tf

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
            # KEY_TAXON: tf.cast(i[KEY_TAXON], tf.bool)
        }

        if num_classes == 2:
            m[KEY_TAXON] = tt.apply_function((lambda l: [0] if l[0] == 0 else [1]), i[KEY_TAXON])
            m[KEY_TAXON] = tf.cast(i[KEY_TAXON], tf.int64)
        else:
            m[KEY_TAXON] = i[KEY_TAXON]

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
