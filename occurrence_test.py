from __future__ import absolute_import

import unittest
import occurrence as fo
from google.cloud.datastore import helpers
from google.cloud import datastore
from datetime import datetime
import logging

# python -m unittest occurrence_test
class OccurrenceParseTest(unittest.TestCase):
    def test(self):

        logging.getLogger().setLevel(logging.INFO)
        client = datastore.Client()
        taxon = datastore.Key('Taxon', 1234, project='floracast-20c01')
        source = datastore.Key('Source', "12|||123", project='floracast-20c01', parent=taxon)
        key = datastore.Key('Occurrence', 4321, project='floracast-20c01', parent=source)
        entity = datastore.Entity(key=key)
        entity['Date'] = datetime(2017,8,3, 0, 0, 0)
        entity['Location'] = helpers.GeoPoint(latitude=36.6316721,longitude=-81.8079655)

        dofn1 = fo.EntityToString()
        res1 = dofn1.process(helpers.entity_to_protobuf(entity))
        for r in res1:

            dofn2 = fo.StringToTaxonSequenceExample()
            res2 = dofn2.process(r)
            for r in res1:

                self.assertEqual(r[0], "1234|||36.63167210|||-81.80796550|||1501736400")
                self.assertEqual(r[1].context.feature['label'].int64_list.value[0], 1234)
                self.assertEqual(r[1].context.feature['date'].int64_list.value[0], long(1501736400))
                self.assertEqual(r[1].context.feature['latitude'].float_list.value[0], 36.6316721)
                self.assertEqual(r[1].context.feature['longitude'].float_list.value[0], -81.8079655)
                self.assertEqual(r[1].context.feature['daylength'].int64_list.value[0], 50248)
                self.assertEqual(r[1].context.feature['mgrs'].bytes_list.value[0], '19')
