import tensorflow as tf
import constants

class SequenceExampleParser():
    def parse(self, file):

        file = open(file, "r")

        context_parsed, sequence_parsed = tf.contrib.layers.parse_feature_columns_from_sequence_examples(
            serialized = file.read(),
            context_feature_columns = {
                'label': tf.FixedLenFeature([], dtype=tf.int64),
                'date': tf.FixedLenFeature([], dtype=tf.int64),
                'latitude': tf.FixedLenFeature([], dtype=tf.float32),
                'longitude': tf.FixedLenFeature([], dtype=tf.float32),
                'daylength': tf.FixedLenFeature([], dtype=tf.int64),
                'mgrs': tf.FixedLenFeature([], dtype=tf.string)
            },
            sequence_feature_columns = {
                'tmax': tf.FixedLenSequenceFeature([constants.WeatherDaysBeforeOccurrence], dtype=tf.float32),
                'tmin': tf.FixedLenSequenceFeature([constants.WeatherDaysBeforeOccurrence], dtype=tf.float32),
                'temp': tf.FixedLenSequenceFeature([constants.WeatherDaysBeforeOccurrence], dtype=tf.float32),
                'prcp': tf.FixedLenSequenceFeature([constants.WeatherDaysBeforeOccurrence], dtype=tf.float32),
            }
        )

        file.close()
        return context_parsed, sequence_parsed
