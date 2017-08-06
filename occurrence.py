from __future__ import absolute_import

import apache_beam as beam
import logging
import astral
from google.cloud.datastore import helpers
from apache_beam.metrics import Metrics
import tensorflow as tf
from google.cloud import datastore
import mgrs
from datetime import datetime


class SequenceExampleCoder(beam.coders.Coder):

    def encode(self, o):
        return o.SerializeToString()

    def decode(self, str):
        se = tf.train.SequenceExample()
        se.ParseFromString(str)

        # context_parsed, sequence_parsed = tf.parse_single_sequence_example(
        #     serialized = o,
        #     context_features = {
        #         'label': tf.FixedLenFeature([], tf.int64),
        #         'date': tf.FixedLenFeature([], tf.int64),
        #         'latitude': tf.FixedLenFeature([], tf.float32),
        #         'longitude': tf.FixedLenFeature([], tf.float32),
        #         'daylength': tf.FixedLenFeature([], tf.int64),
        #         'mgrs': tf.FixedLenFeature([], tf.string)
        #     },
        #     sequence_features = {
        #         'tmax': tf.FixedLenSequenceFeature([], tf.float32),
        #         'tmin': tf.FixedLenSequenceFeature([], tf.float32),
        #         'temp': tf.FixedLenSequenceFeature([], tf.float32),
        #         'prcp': tf.FixedLenSequenceFeature([], tf.float32),
        #     })
        #
        # se = tf.train.SequenceExample()
        # for k, v in context_parsed:
        #
        # se.context = tf.train.Features(feature=context_parsed)
        # se.feature_lists = tf.train.FeatureLists(feature_list=sequence_parsed)
        return se

    def is_deterministic(self):
        return True


beam.coders.registry.register_coder(tf.train.SequenceExample, SequenceExampleCoder)

# Filter and prepare for duplicate sort.
@beam.typehints.with_input_types(datastore.Entity)
@beam.typehints.with_output_types(str)
class EntityToString(beam.DoFn):
    def __init__(self):
        super(EntityToString, self).__init__()
        self.new_occurrence_counter = Metrics.counter('main', 'new_occurrences')
        self.invalid_occurrence_date = Metrics.counter('main', 'invalid_occurrence_date')
        self.invalid_occurrence_location = Metrics.counter('main', 'invalid_occurrence_location')

    def process(self, element):
        """
            Element should be an occurrence entity.
            The key has should be a sufficient key.
        """
        e = helpers.entity_from_protobuf(element)

        self.new_occurrence_counter.inc()

        if e.key.parent.parent.id != 236714:
            return

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

        yield "%d|||%.8f|||%.8f|||%d" % (e.key.parent.parent.id, lat, lng, int(e['Date'].strftime("%s")))

@beam.typehints.with_input_types(str)
@beam.typehints.with_output_types(beam.typehints.KV[int, tf.train.SequenceExample])
class StringToTaxonSequenceExample(beam.DoFn):
    def __init__(self):
        super(StringToTaxonSequenceExample, self).__init__()

    def process(self, element):

        ls = element.split("|||")
        taxon = int(ls[0])
        lat = float(ls[1])
        lng = float(ls[2])
        intDate = int(ls[3])
        date = datetime.fromtimestamp(intDate)

        daylength = 0
        try:
            a = astral.Astral()
            a.solar_depression = 'civil'
            astro = a.sun_utc(date, lat, lng)
            daylength = (astro['sunset'] - astro['sunrise']).seconds
        except astral.AstralError as err:
            if "Sun never reaches 6 degrees below the horizon" in err.message:
                daylength = 86400
            else:
                logging.error("Error parsing day[%s] length at [%.6f,%.6f]: %s", date, lat, lng, err)
                return

        se = tf.train.SequenceExample()
        se.context.feature["label"].int64_list.value.append(taxon)
        se.context.feature["latitude"].float_list.value.append(lat)
        se.context.feature["longitude"].float_list.value.append(lng)
        se.context.feature["date"].int64_list.value.append(intDate)
        se.context.feature["daylength"].int64_list.value.append(daylength)
        se.context.feature["mgrs"].bytes_list.value.append(mgrs.MGRS().toMGRS(lat, lng)[:2])

        yield taxon, se