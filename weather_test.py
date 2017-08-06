from __future__ import absolute_import

import unittest
import occurrence as fo
import tensorflow as tf
import datetime
import weather as fw

class WeatherParseTest(unittest.TestCase):
    def test(self):
    # INFO:root:range: 49.13330000, -96.76670000, 1994, set(['08', '07'])
        se = tf.train.SequenceExample()
        se.context.feature["label"].int64_list.value.append(1)
        se.context.feature["latitude"].float_list.value.append(49.13330000)
        se.context.feature["longitude"].float_list.value.append(-96.76670000)
        se.context.feature["date"].int64_list.value.append(int(datetime.datetime(1994, 8, 15).strftime("%s")))
        se.context.feature["daylength"].int64_list.value.append(1240)

        dofn = fw.FetchWeatherDoFn("floracast-20c01")
        res = dofn.process(se)

        for r in res:
            print(r)