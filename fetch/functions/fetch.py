# from __future__ import absolute_import
# from __future__ import division

import apache_beam as beam
from google.cloud.firestore_v1beta1 import client
from .example import Example, ParseExampleFromFirestore

@beam.typehints.with_input_types(beam.typehints.Tuple[str, str, str])
@beam.typehints.with_output_types(Example)
class FetchOccurrences(beam.DoFn):
    def __init__(self, project):
        super(FetchOccurrences, self).__init__()
        self._project = project

    def process(self, (nameusage_id, source_type, target_id)):

        # nameusage_id = unicode(request[0], "utf-8")
        # source_type = unicode(request[1], "utf-8")
        # target_id = unicode(request[2], "utf-8")

        q = client.Client(project=self._project).collection(u'Occurrences')
        q = q.where(u'SourceType', u'==', source_type)
        q = q.where(u'TargetID', u'==', target_id)

        for o in q.get():
            yield ParseExampleFromFirestore(nameusage_id, o.id, o.to_dict())

@beam.typehints.with_input_types(int)
@beam.typehints.with_output_types(beam.typehints.Tuple[str, str, str])
class FetchNameUsages(beam.DoFn):
    def __init__(self, project, nameusages):
        super(FetchNameUsages, self).__init__()
        self._project = project
        self._nameusages = nameusages

    def process(self, i):

        nameusages = self._nameusages.get()
        if nameusages is None or nameusages == "":
            return

        for nameusage_id in nameusages.split(","):

            nameusage_id = unicode(nameusage_id, "utf-8")

            fields = client.Client(project=self._project).collection(u'NameUsages').document(nameusage_id).get().to_dict()

            if fields["Sources"] is None:
                print(ValueError("Sources field should not be empty in NameUsage"), nameusage_id)
                continue

            for source_type in fields["Sources"]:
                for target_id in fields["Sources"][source_type]:
                    source = fields["Sources"][source_type][target_id]
                    if "Occurrences" not in source:
                        continue
                    if source["Occurrences"] is not None and source["Occurrences"] > 0:
                        yield (nameusage_id, source_type, target_id)

@beam.typehints.with_input_types(int)
@beam.typehints.with_output_types(Example)
class FetchRandom(beam.DoFn):
    def __init__(self, project, should_fetch):
        super(FetchRandom, self).__init__()
        self._project = project
        self._should_fetch = should_fetch

    def process(self, i):
        should_fetch = self._should_fetch.get()
        if should_fetch is None or should_fetch is False:
            return
        for o in client.Client(project=self._project).collection(u'Random').get():
            r = o.to_dict()
            yield ParseExampleFromFirestore("Random", o.id, o.to_dict())

@beam.typehints.with_input_types(int)
@beam.typehints.with_output_types(Example)
class FetchProtectedAreas(beam.DoFn):
    def __init__(self, project, protected_area_dates):
        super(FetchProtectedAreas, self).__init__()
        self._project = project
        self._protected_area_dates = protected_area_dates

    def process(self, i):

        dates = self._protected_area_dates.get()
        if dates is None or dates.strip() == "":
            return

        date_list = dates.split(",")
        for area in client.Client(project=self._project).collection(u'ProtectedArea').get():
            pa = area.to_dict()
            for d in date_list:
                pa["FormattedDate"] = d
                id = pa.id + "-" + d
                yield ParseExampleFromFirestore("ProtectedArea-"+d, id, pa)
