# from __future__ import absolute_import

import apache_beam as beam
import constants


def parse_pipeline_argument(arg):
    print("type", type(arg))

    if type(arg) is tuple:
        return arg[0]
    elif hasattr(arg, 'get'):
        return arg.get()
    else:
        return arg

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

@beam.ptransform_fn
def GroupByRegionDate(pcoll):  # pylint: disable=invalid-name
    return (pcoll
            | 'ProjectDateToDefuse' >> beam.Map(lambda e: (e.month_region_string(), e))
            | 'GroupByKeyToDiffuse' >> beam.GroupByKey())

@beam.ptransform_fn
def DiffuseByMonth(pcoll):  # pylint: disable=invalid-name
    return (pcoll
            | 'ProjectDateToDefuse' >> beam.Map(lambda e: (e.month_string(), e))
            | 'GroupByKeyToDiffuse' >> beam.GroupByKey()
            | 'UngroupToDefuse' >> beam.FlatMap(lambda v: v[1]))

@beam.ptransform_fn
def DiffuseByYear(pcoll):  # pylint: disable=invalid-name
    return (pcoll
            | 'ProjectDateToDefuse' >> beam.Map(lambda e: (e.year_string(), e))
            | 'GroupByKeyToDiffuse' >> beam.GroupByKey()
            | 'UngroupToDefuse' >> beam.FlatMap(lambda v: v[1]))


@beam.ptransform_fn
def RemoveOccurrenceExampleLocationDuplicates(pcoll):  # pylint: disable=invalid-name
    """Produces a PCollection containing the unique elements of a PCollection."""
    return pcoll \
           | 'ToPairs' >> beam.Map(lambda e: (e.equality_key(), e)) \
           | 'GroupByKey' >> beam.GroupByKey() \
           | 'Combine' >> beam.Map(lambda (key, examples): list(examples)[0])


@beam.typehints.with_input_types(beam.typehints.Tuple[str, beam.typehints.Iterable[beam.typehints.Any]])
@beam.typehints.with_output_types(beam.typehints.Any)
class Truncate(beam.DoFn):
    def process(self, kv, occurrence_total=0):
        count = 0
        for e in kv[1]:
            count = count + 1
            yield e
            if count > occurrence_total:
                return

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
    m = hashlib.md5(str(ex[constants.KEY_EXAMPLE_ID]) + str(partition_random_seed))
    hash_value = int(m.hexdigest(), 16) % 100
    return 0 if hash_value >= percent_eval else 1

# class NoOperation(beam.DoFn):
#     def __init__(self):
#         return
#     def process(self, element):
#         yield element