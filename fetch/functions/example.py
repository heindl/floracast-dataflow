# from __future__ import absolute_import
import apache_beam as beam
import calendar
from tensorflow.core.example import example_pb2
from datetime import datetime
from .geospatial import GeospatialBounds
import tensorflow as tf

KEY_EXAMPLE_ID = 'example_id'
KEY_CATEGORY = 'nameusage'
KEY_LATITUDE = 'latitude'
KEY_LONGITUDE = 'longitude'
KEY_ELEVATION = 'elevation'
KEY_DATE ='date'
KEY_AVG_TEMP = 'avg_temp'
KEY_MAX_TEMP = 'max_temp'
KEY_MIN_TEMP = 'min_temp'
KEY_PRCP = 'precipitation'
KEY_DAYLIGHT = 'daylight'
KEY_S2_CELLS = 's2_cells'
KEY_ECO_BIOME = 'eco_biome'
KEY_ECO_REALM = 'eco_realm'
KEY_ECO_NUM = 'eco_num'

class ExampleCoder(beam.coders.Coder):

    def __init__(self):
        super(ExampleCoder, self).__init__()

    def encode(self, example):
        return example.encode()

    def decode(self, s):
        return Example().decode_from_string(s)

    def is_deterministic(self):
        return True

# LIST_TYPES
LIST_TYPE_INT64 = 'int64'
LIST_TYPE_FLOAT = 'float'
LIST_TYPE_BYTES = 'bytes'


class Season:

    January, February, March, April, May, June, July, August, September, October, November, December = range(1, 13)

    Spring, Summer, Autumn, Winter = range(1, 5)

    def __init__(self, season=None):
        if season is not None:
            self._season = season

    def _months(self):
        return {
            self.Spring: [self.March, self.April, self.May],
            self.Summer: [self.June, self.July, self.August],
            self.Autumn: [self.September, self.October, self.November],
            self.Winter: [self.December, self.January, self.February]
        }[self.as_int()]

    def as_int(self):
        if self._is_valid() is False:
            raise ValueError("Invalid Season")
        return self._season

    def _is_valid(self):
        return self._season is not None and self._season >= 1 and self._season <= 4

    def last_month(self):
        return self._months()[2]

    def from_month(self, m):
        if m < 1 or m > 12:
            raise ValueError("Month should be between 1 and 12")

        for s in [self.Spring, self.Summer, self.Autumn, self.Winter]:
            self._season = s
            if m in self._months():
                return self


class Examples:

    def __init__(self, example_list=None):

        if type(example_list) is not list:
            raise ValueError('Expected type list to create Examples')

        if len(example_list) == 0:
            raise ValueError('Expected at least one example in list to create Examples')
        self._list = []

        self._list = example_list
        self._parse()

    def count(self):
        return len(self._list)

    def write(self, filepath):
        options = tf.python_io.TFRecordOptions(
            compression_type=tf.python_io.TFRecordCompressionType.GZIP
        )
        record_writer = tf.python_io.TFRecordWriter(path=filepath, options=options)
        for e in self._list:
            record_writer.write(e.encode())
        record_writer.close()

    def _parse(self):

        self._south, self._north, self._west, self._east = 0, 0, 0, 0
        self._earliest_date, self._latest_date = 0, 0

        for e in self._list:
            if self._south is 0 or e.latitude() < self._south:
                self._south = e.latitude()

            if self._north is 0 or e.latitude() > self._north:
                self._north = e.latitude()

            if self._east is 0 or e.longitude() > self._east:
                self._east = e.longitude()

            if self._west is 0 or e.longitude() < self._west:
                self._west = e.longitude()

            date_int = int(e.date())

            if self._earliest_date is 0 or date_int < self._earliest_date:
                self._earliest_date = date_int

            if self._latest_date is 0 or date_int > self._latest_date:
                self._latest_date = date_int

    def bounds(self):
        return GeospatialBounds().from_coordinates(self._north, self._east, self._south, self._west)

    def earliest_datetime(self):
        return datetime.strptime(str(self._earliest_date), '%Y%m%d')

    def latest_datetime(self):
        return datetime.strptime(str(self._latest_date), '%Y%m%d')

    def as_list(self):
        return self._list

    def in_batches(self, n):
        """Yield successive n-sized chunks from l."""
        for i in xrange(0, len(self._list), n):
            yield self._list[i:i + n]

class Example:

    def __init__(self, example=None):
        if example is not None:
            self._example = example
        else:
            self._example = example_pb2.Example()

    def _append_value(self, feature, typer, value):
        if typer == LIST_TYPE_INT64:
            self._example.features.feature[feature].int64_list.value.append(value)
        elif typer == LIST_TYPE_FLOAT:
            self._example.features.feature[feature].float_list.value.append(value)
        elif typer == LIST_TYPE_BYTES:
            self._example.features.feature[feature].bytes_list.value.append(value)

    def as_map(self):
        v = {}
        for k in self._example.features.feature:
            if len(self._example.features.feature[k].int64_list.value) > 0:
                v[k] = self._example.features.feature[k].int64_list.value
                continue
            if len(self._example.features.feature[k].float_list.value) > 0:
                v[k] = self._example.features.feature[k].float_list.value
                continue
            if len(self._example.features.feature[k].bytes_list.value) > 0:
                v[k] = self._example.features.feature[k].bytes_list.value
                continue
        return v

    def _set_value(self, feature, typer, value, i=0):
        if typer == LIST_TYPE_INT64:
            if len(self._example.features.feature[feature].int64_list.value) <= i:
                self._append_value(feature, typer, value)
            else:
                self._example.features.feature[feature].int64_list.value[i] = value
        elif typer == LIST_TYPE_FLOAT:
            if len(self._example.features.feature[feature].float_list.value) <= i:
                self._append_value(feature, typer, value)
            else:
                self._example.features.feature[feature].float_list.value[i] = value
        elif typer == LIST_TYPE_BYTES:
            if len(self._example.features.feature[feature].bytes_list.value) <= i:
                self._append_value(feature, typer, value)
            else:
                self._example.features.feature[feature].bytes_list.value[i] = value

    def _get_value(self, feature, typer, i=0):
        v = self._get_values(feature, typer)
        if v is not None:
            return v[i]
        return None

    def _get_values(self, feature, typer):

        f = self._example.features.feature[feature]

        v = []
        if typer == LIST_TYPE_INT64:
            v = f.int64_list.value
        elif typer == LIST_TYPE_FLOAT:
            v = f.float_list.value
        elif typer == LIST_TYPE_BYTES:
            v = f.bytes_list.value
        else:
            return None

        if len(v) == 0:
            return None

        return v

    def example_id(self):
        return self._get_value(KEY_EXAMPLE_ID, LIST_TYPE_BYTES)

    def set_example_id(self, example_id):
        self._set_value(KEY_EXAMPLE_ID, LIST_TYPE_BYTES, example_id)

    def category(self):
        c = self._get_value(KEY_CATEGORY, LIST_TYPE_BYTES)
        if c is None or c == "":
            raise ValueError('Invalid example category')
        return c

    def set_category(self, category):
        if category == "":
            raise ValueError('Invalid example category')
        self._set_value(KEY_CATEGORY, LIST_TYPE_BYTES, category)

    def latitude(self):
        lat = self._get_value(KEY_LATITUDE, LIST_TYPE_FLOAT)
        if lat == 0:
            raise ValueError('Invalid example latitude')
        return lat

    def set_latitude(self, lat):
        if lat == 0:
            raise ValueError('Invalid example latitude')
        self._set_value(KEY_LATITUDE, LIST_TYPE_FLOAT, lat)

    def longitude(self):
        lng = self._get_value(KEY_LONGITUDE, LIST_TYPE_FLOAT)
        if lng == 0:
            raise ValueError('Invalid example longitude')
        return lng

    def set_longitude(self, lng):
        if lng == 0:
            raise ValueError('Invalid example longitude')
        self._set_value(KEY_LONGITUDE, LIST_TYPE_FLOAT, lng)

    # def _set_grid_zone(self):
    #     import mgrs
    #     lat = self.latitude()
    #     lng = self.longitude()
    #     if lat is not None and lng is not None:
    #         self._set_value(KEY_GRID_ZONE, LIST_TYPE_BYTES, mgrs.MGRS().toMGRS(lat, lng)[:2].encode())

    def date(self):
        date = self._get_value(KEY_DATE, LIST_TYPE_BYTES)
        if len(date) != 8:
            raise ValueError('Example date should be in format 20060102')
        return date

    def datetime(self):
        return datetime.strptime(self.date(), '%Y%m%d')

    def set_date(self, date):
        if len(date) != 8:
            raise ValueError('Example date should be in format 20060102')
        self._set_value(KEY_DATE, LIST_TYPE_BYTES, date)

    def date_split(self):
        d = self.date()
        return (d[0:4], d[4:6], d[6:8])

    def year(self):
        d = self.date_split()
        return int(d[0])

    def year_string(self):
        d = self.date_split()
        return d[0]

    def month_string(self):
        d = self.date_split()
        return d[1]

    def month(self):
        d = self.date_split()
        return int(d[1])

    def month_name(self):
        return calendar.month_name[self.month()]

    def month_region_key(self):
        return self.year_string() + "-" + self.month_string() + "-" + self.s2_cell(3)

    def season(self):
        return Season().from_month(self.month())

    def season_region_key(self):
        return '%s-%d-%s' % (self.year_string(), self.season().as_int(), self.s2_cell(2))

    def set_s2_cell(self, cells):
        if len(cells) != 7:
            raise ValueError('Example requires eight S2 cells')

        if type(cells) == list:
            for i, c in enumerate(cells):
                self._set_value(KEY_S2_CELLS, LIST_TYPE_BYTES, c, int(i))
            return

        for k in cells:
            self._set_value(KEY_S2_CELLS, LIST_TYPE_BYTES, str(cells[k]), int(k))

    def s2_cell(self, level):
        cell = self._get_value(KEY_S2_CELLS, LIST_TYPE_BYTES, level)
        if cell == "":
            raise ValueError("S2Cell missing at level:", level)
        return cell

    def s2_cells(self):
        return self._get_values(KEY_S2_CELLS, LIST_TYPE_BYTES)

    def eco_region(self):
        return (
            int(self._get_value(KEY_ECO_REALM, LIST_TYPE_INT64)),
            int(self._get_value(KEY_ECO_BIOME, LIST_TYPE_INT64)),
            int(self._get_value(KEY_ECO_NUM, LIST_TYPE_INT64))
        )

    def set_eco_region(self, realm, biome, num):
        if realm == 0 or biome == 0 or num == 0:
            raise ValueError('Realm, Biome and EcoNum can not be empty')
        self._set_value(KEY_ECO_REALM, LIST_TYPE_INT64, realm)
        self._set_value(KEY_ECO_BIOME, LIST_TYPE_INT64, biome)
        self._set_value(KEY_ECO_NUM, LIST_TYPE_INT64, num)

    def coordinates(self):
        return self.latitude(), self.longitude()

    def elevation(self):
        return int(self._get_value(KEY_ELEVATION, LIST_TYPE_INT64))

    def set_elevation(self, elevation):
        self._set_value(KEY_ELEVATION, LIST_TYPE_INT64, elevation)

    def as_pb2(self):
        return self._example

    def as_dict(self):
        return self._example.features

    def encode(self):
        return self._example.SerializeToString()

    def decode_from_string(self, s):
        self._example.ParseFromString(s)

    def equality_key(self):
        return "%s|||%.4f|||%.4f|||%s" % (
            self.category(),
            self.latitude(),
            self.longitude(),
            self.date()
        )


    def append_temp_avg(self, value):
        self._append_value(KEY_AVG_TEMP, LIST_TYPE_FLOAT, value)

    def append_temp_max(self, value):
        self._append_value(KEY_MAX_TEMP, LIST_TYPE_FLOAT, value)

    def append_temp_min(self, value):
        self._append_value(KEY_MIN_TEMP, LIST_TYPE_FLOAT, value)

    def append_precipitation(self, value):
        self._append_value(KEY_PRCP, LIST_TYPE_FLOAT, value)

    def append_daylight(self, value):
        self._append_value(KEY_DAYLIGHT, LIST_TYPE_FLOAT, value)



def FromSerialized(serialized):
    e = Example()
    e.decode_from_string(serialized)
    return e

def ParseExampleFromFirestore(category_id, example_id, o):
    e = Example()

    e.set_category(str(category_id))
    e.set_example_id(str(example_id))

    # This is a hack to avoid indexing the 'Date' property in Go.
    # 20170215: Should already be done.
    # if int(occurrence['FormattedDate']) < 19700101:
    #     continue
    e.set_date(str(o['FormattedDate']))

    e.set_latitude(float(o['GeoPoint']['latitude']))
    e.set_longitude(float(o['GeoPoint']['longitude']))

    if 'Elevation' not in o:
        raise ValueError('Elevation must be set in Firestore Occurrence')

    e.set_elevation(int(o['Elevation']))

    e.set_eco_region(int(o["EcoRealm"]), int(o["EcoBiome"]), int(o["EcoNum"]))

    e.set_s2_cell(o['S2Tokens'])

    return e


    # (lat, lng) = (0.0, 0.0)
    # if type(loc) is GeoPoint:
    #     lat = loc.latitude
    #     lng = loc.longitude
    # elif type(loc) is Entity:
    #     lat = loc['Lat']
    #     lng = loc['Lng']
    # else:
    #     logging.error("invalid type: %s", type(loc))
    #     return