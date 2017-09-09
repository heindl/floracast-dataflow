# from __future__ import absolute_import
import apache_beam as beam
from google.cloud.proto.datastore.v1 import entity_pb2
from example import Example


# Filter and prepare for duplicate sort.
@beam.typehints.with_input_types(entity_pb2.Entity)
@beam.typehints.with_output_types(Example)
class ForestEntityToExample(beam.DoFn):

    def __init__(self, friday=None, periods=1):
        from datetime import datetime, timedelta
        super(ForestEntityToExample, self).__init__()
        self._friday = friday
        if friday is None:
            today = datetime.now()
            self._friday = today + timedelta((4 - today.weekday()) % 7)
        self._periods = periods

    def _parse_point(self, loc):
        from geopy import Point
        from google.cloud.datastore.helpers import GeoPoint
        from google.cloud.datastore import Entity

        if type(loc) is GeoPoint:
            return Point(loc.latitude, loc.longitude)
        elif type(loc) is Entity:
            return Point(loc['Lat'], loc['Lng'])
        else:
            raise ValueError('Could not parse Geopoint')

    def process(self, element):
        from google.cloud.datastore.helpers import entity_from_protobuf
        from pandas import date_range
        """
            Element should be an occurrence entity.
            The key has should be a sufficient key.
        """
        e = entity_from_protobuf(element)
        centre = self._parse_point(e['Centre'])
        for d in date_range(end=self._friday, periods=self._periods, freq='W').tolist():
            intDate = int(d.strftime("%s"))
            ex = Example()
            ex.set_occurrence_id("%d|||%d" % (e.key.id, intDate))
            ex.set_longitude(centre.longitude)
            ex.set_latitude(centre.latitude)
            ex.set_date(intDate)
            yield ex


class ReadDatastoreForests(beam.PTransform):
    """Wrapper for reading from either CSV files or from BigQuery."""

    def __init__(self, project):
        super(ReadDatastoreForests, self).__init__()
        self._project = project

    def expand(self, pvalue):
        from apache_beam.io.gcp.datastore.v1.datastoreio import ReadFromDatastore
        from google.cloud.proto.datastore.v1 import query_pb2

        q = query_pb2.Query()
        q.kind.add().name='WildernessArea'

        return (pvalue.pipeline
                | 'ReadDatastoreForests' >> ReadFromDatastore(
            project=self._project,
            query=q
            )
        )