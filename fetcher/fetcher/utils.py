# from __future__ import absolute_import

import apache_beam as beam

# def encode_as_b64_json(serialized_example):
#     import base64  # pylint: disable=g-import-not-at-top
#     import json  # pylint: disable=g-import-not-at-top
#     return json.dumps({'b64': base64.b64encode(serialized_example)})

# # TODO: Perhaps use Reshuffle (https://issues.apache.org/jira/browse/BEAM-1872)?
@beam.ptransform_fn
def Shuffle(pcoll):  # pylint: disable=invalid-name
    import random
    return (pcoll
            | 'PairWithRandom' >> beam.Map(lambda x: (random.random(), x))
            | 'GroupByRandom' >> beam.GroupByKey()
            | 'DropRandom' >> beam.FlatMap(lambda (k, vs): vs))



# class NoOperation(beam.DoFn):
#     def __init__(self):
#         return
#     def process(self, element):
#         yield element