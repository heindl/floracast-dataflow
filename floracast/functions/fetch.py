# from __future__ import absolute_import
# from __future__ import division

import apache_beam as beam
from google.cloud import firestore, exceptions
from example import Example, ParseExampleFromFirestore
import logging
from utils import parse_pipeline_argument
import copy

# @beam.typehints.with_input_types(beam.typehints.Tuple[str, str, str])
@beam.typehints.with_input_types(str)
@beam.typehints.with_output_types(Example)
class FetchOccurrences(beam.DoFn):
    def __init__(self, project):
        super(FetchOccurrences, self).__init__()
        self._project = project

    # def process(self, (nameusage_id, source_type, target_id)):
    def process(self, s):

        data = s.split("-|-")

        # nameusage_id = unicode(data[0], "utf-8")
        # source_type = unicode(data[1], "utf-8")
        # target_id = unicode(data[2], "utf-8")

        nameusage_id = data[0]
        source_type = data[1]
        target_id = data[2]

        logging.debug("Fetching Occurrences from FireStore: %s, %s, %s", nameusage_id, source_type, target_id)

        db = firestore.Client(project=self._project)
        col = db.collection(u'Occurrences')
        q = col.where(u'SourceType', u'==', unicode(source_type, "utf-8")).where(u'TargetID', u'==', unicode(target_id, "utf-8"))

        # logging.debug("Received %d Occurrences from Firestore: %s, %s, %s", len(occurrences), nameusage_id, source_type, target_id)

        for o in q.get():
            logging.debug("Parsing Occurrence [%s]", o.id)
            try:
                e = ParseExampleFromFirestore(nameusage_id, o.id, o.to_dict())
            except ValueError as error:
                logging.error('Occurrence [%s] could not be parsed into Example: %s', o.id, error)
                continue
            yield e


# @beam.typehints.with_output_types(beam.typehints.Tuple[str, str, str])
@beam.typehints.with_input_types(int)
@beam.typehints.with_output_types(str)
class FetchNameUsages(beam.DoFn):
    def __init__(self, project, nameusages):
        super(FetchNameUsages, self).__init__()
        self._project = project
        self._nameusages = nameusages

    def process(self, i=0):

        nameusages = parse_pipeline_argument(self._nameusages)

        if nameusages is None or nameusages == "":
            return

        for nameusage_id in nameusages.split(","):

            nameusage_id.strip()

            logging.debug("Fetching NameUsage [%s] from Firestore", nameusage_id)

            usage = firestore.Client(project=self._project).document(u'NameUsages/%s' % nameusage_id).get()

            logging.debug("Received NameUsage [%s] from Firestore", nameusage_id)

            usage_fields = usage.to_dict()

            if usage_fields["Sources"] is None:
                logging.error("NameUsage [%s] has empty Sources Field", nameusage_id)
                continue

            for source_type in usage_fields["Sources"]:
                for target_id in usage_fields["Sources"][source_type]:
                    source = usage_fields["Sources"][source_type][target_id]
                    if "Occurrences" not in source:
                        continue
                    if source["Occurrences"] is not None and source["Occurrences"] > 0:
                        res = str("%s-|-%s-|-%s" % (nameusage_id, source_type, target_id))
                        logging.debug("Yielding Occurrence Source: %s", res)
                        yield res

ORDER_BY_VALUE = u'GeoFeatureSet.ModifiedAt'

@beam.typehints.with_input_types(beam.typehints.Tuple[long, long])
@beam.typehints.with_output_types(Example)
class FetchRandom(beam.DoFn):
    def __init__(self, project):
        super(FetchRandom, self).__init__()
        self._project = project

    def process(self, offset):

        ref = firestore.Client(project=self._project).collection(u'Random')

        start = list(ref.where(ORDER_BY_VALUE, "==", offset[0]).get())[0]

        q = ref.order_by(ORDER_BY_VALUE).start_at(start)

        if offset[1] != 0:
            end = list(ref.where(ORDER_BY_VALUE, "==", offset[1]).get())[0]
            q = q.end_before(end)

        for o in q.get():
            try:
                e = ParseExampleFromFirestore("random", o.id, o.to_dict())
            except ValueError as error:
                logging.error('Random Occurrence [%s] could not be parsed into Example: %s', o.id, error)
                continue
            yield e

@beam.typehints.with_input_types(int)
@beam.typehints.with_output_types(beam.typehints.Tuple[long, long]) # Start, End
class GeneratePointBatches(beam.DoFn):
    def __init__(self, project, collection, engage):
        super(GeneratePointBatches, self).__init__()
        self._project = project
        self._collection = collection
        self._engage = engage

    def process(self,i=0):

        engage = parse_pipeline_argument(self._engage)
        if engage is not True:
            return

        q = firestore.Client(project=self._project).collection(self._collection).order_by(ORDER_BY_VALUE)

        startRef = list(q.limit(1).get())[0]

        while True:
            try:
                endRef = list(q
                    .start_at(startRef)
                    .offset(250)
                    .limit(1)
                    .get())[0]
                yield [startRef.get(ORDER_BY_VALUE), endRef.get(ORDER_BY_VALUE)]
            except IndexError:
                yield [startRef.get(ORDER_BY_VALUE), 0]
                return
            startRef = endRef



@beam.typehints.with_input_types(beam.typehints.Tuple[long, long])
@beam.typehints.with_output_types(beam.typehints.Dict[str, beam.typehints.Any])
class FetchProtectedAreas(beam.DoFn):
    def __init__(self, project):
        super(FetchProtectedAreas, self).__init__()
        self._project = project

    def process(self, offset):

        ref = firestore.Client(project=self._project).collection(u'ProtectedAreas')

        start = list(ref.where(ORDER_BY_VALUE, "==", offset[0]).get())[0]

        q = ref.order_by(ORDER_BY_VALUE).start_at(start)

        if offset[1] != 0:
            end = list(ref.where(ORDER_BY_VALUE, "==", offset[1]).get())[0]
            q = q.end_before(end)

        for o in q.get():
            yield o.to_dict()


@beam.typehints.with_input_types(beam.typehints.Dict[str, beam.typehints.Any])
@beam.typehints.with_output_types(Example)
class ExplodeProtectedAreaDates(beam.DoFn):
    def __init__(self, protected_area_dates):
        super(ExplodeProtectedAreaDates, self).__init__()
        self._protected_area_dates = protected_area_dates

    def process(self, area):

        dates = parse_pipeline_argument(self._protected_area_dates)
        if dates is None or dates.strip() == "":
            return

        for d in dates.split(","):
            area_copy = copy.deepcopy(area)

            area_copy["FormattedDate"] = d
            try:
                e = ParseExampleFromFirestore("protected_area-"+d, area_copy["GeoFeatureSet"]["S2Tokens"]["15"], area_copy)
            except ValueError as error:
                logging.error('ProtectedArea [%s] could not be parsed into Example: %s', area_copy["GeoFeatureSet"]["S2Tokens"]["15"], error)
                continue
            yield e
