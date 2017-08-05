from __future__ import absolute_import

import apache_beam as beam
import logging
import astral
from google.cloud.datastore import helpers
from apache_beam.metrics import Metrics
import tensorflow as tf
from google.cloud import datastore

class Occurrence(object):
    def __init__(self, example):
        self.example = example

    def __eq__(self, other):

        s = self.example.context.feature
        n = other.example.context.feature

        if s['label'].int64_list.value[0] != n['label'].int64_list.value[0]:
            return False

        if s['date'].int64_list.value[0] != n['date'].int64_list.value[0]:
            return False

        if s['latitude'].float_list.value[0] != n['latitude'].float_list.value[0]:
            return False

        if s['longitude'].float_list.value[0] != n['longitude'].float_list.value[0]:
            return False

        return True


class OccurrenceCoder(beam.coders.Coder):

    def encode(self, o):
        return o.example.SerializeToString()

    def decode(self, o):

        context_parsed, sequence_parsed = tf.parse_single_sequence_example(
            serialized = o,
            context_features = {
                'label': tf.FixedLenFeature([], tf.int64),
                'date': tf.FixedLenFeature([], tf.int64),
                'latitude': tf.FixedLenFeature([], tf.float64),
                'longitude': tf.FixedLenFeature([], tf.float64),
                'daylength': tf.FixedLenFeature([], tf.int64),
            },
            sequence_features = {
                'tmax': tf.FixedLenSequenceFeature([], tf.float32),
                'tmin': tf.FixedLenSequenceFeature([], tf.float32),
                'temp': tf.FixedLenSequenceFeature([], tf.float32),
                'prcp': tf.FixedLenSequenceFeature([], tf.float32),
            })
        return Occurrence(tf.train.SequenceExample(features=context_parsed, feature_lists=sequence_parsed))

    def is_deterministic(self):
        return True


beam.coders.registry.register_coder(Occurrence, OccurrenceCoder)


@beam.typehints.with_input_types(datastore.Entity)
@beam.typehints.with_output_types(Occurrence)
class EntityToOccurrence(beam.DoFn):
    def __init__(self):
        super(EntityToOccurrence, self).__init__()
        self.new_occurrence_counter = Metrics.counter('main', 'new_occurrences')
        self.invalid_occurrence_date = Metrics.counter('main', 'invalid_occurrence_date')
        self.invalid_occurrence_location = Metrics.counter('main', 'invalid_occurrence_location')

    def process(self, element):
        """
            Element should be an occurrence entity.
            The key has should be a sufficient key.
        """
        e = helpers.entity_from_protobuf(element)

        # This is a hack to avoid indexing the 'Date' property in Go.
        if e['Date'].year < 1970:
            self.invalid_occurrence_date.inc()
            return

        loc = e['Location']

        (lat, lng) = (0.0, 0.0)
        if type(loc) is helpers.GeoPoint:
            lat = loc.latitude
            lng = loc.longitude
        elif type(loc) is datastore.entity.Entity:
            lat = loc['Lat']
            lng = loc['Lng']
        else:
            logging.error("invalid type: %s", type(loc))
            return

        if lng > -52.2330:
            logging.info("%.6f && %.6f", lat, lng)
            self.invalid_occurrence_location.inc()
            return

        daylength = 0
        try:
            a = astral.Astral()
            a.solar_depression = 'civil'
            astro = a.sun_utc(e['Date'], lat, lng)
            daylength = (astro['sunset'] - astro['sunrise']).seconds
        except astral.AstralError as err:
            logging.error("Error parsing day[%s] length at [%.6f,%.6f]: %s", e['Date'], lat, lng, err)
            return

        self.new_occurrence_counter.inc()

        se = tf.train.SequenceExample()
        se.context.feature["label"].int64_list.value.append(e.key.parent.parent.id)
        se.context.feature["latitude"].float_list.value.append(lat)
        se.context.feature["longitude"].float_list.value.append(lng)
        se.context.feature["date"].int64_list.value.append(int(e['Date'].strftime("%s")))
        se.context.feature["daylength"].int64_list.value.append(int(daylength))

        yield Occurrence(se)