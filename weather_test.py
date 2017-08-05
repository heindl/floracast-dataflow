from __future__ import absolute_import

import unittest
import occurrence as fo
import tensorflow as tf
import datetime
import weather as fw

class WeatherParseTest(unittest.TestCase):
    def test(self):

        se = tf.train.SequenceExample()
        se.context.feature["label"].int64_list.value.append(1)
        se.context.feature["latitude"].float_list.value.append(33.7490)
        se.context.feature["longitude"].float_list.value.append(-84.3880)
        se.context.feature["date"].int64_list.value.append(int(datetime.datetime(1984, 11, 15).strftime("%s")))
        se.context.feature["daylength"].int64_list.value.append(1240)

        dofn = fw.FetchWeatherDoFn("floracast-20c01")
        res = dofn.process(fo.Occurrence(se))

        for r in res:
            print(r)
            print(r.example.feature_lists.feature_list["temp"])