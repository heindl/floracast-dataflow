# from __future__ import absolute_import
import apache_beam as beam

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
# KEY_ECO_REALM = 'eco_realm'
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

class Example:

    def __init__(self, example=None):
        from tensorflow.core.example import example_pb2
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
            self._example.features.feature[feature].int64_list.value[i] = value
        elif typer == LIST_TYPE_FLOAT:
            self._example.features.feature[feature].float_list.value[i] = value
        elif typer == LIST_TYPE_BYTES:
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

    def set_date(self, date):
        if len(date) != 8:
            raise ValueError('Example date should be in format 20060102')
        self._set_value(KEY_DATE, LIST_TYPE_BYTES, date)

    def date_split(self):
        d = self.date()
        return (d[:4], d[5:6], d[7:8])

    def year_string(self):
        d = self.date_split()
        return d[0]

    def month_string(self):
        d = self.date_split()
        return d[1]

    def month_region_key(self):
        return self.year_string() + "-" + self.month_string() + "-" + self.s2_cell(3)

    def set_s2_cell(self, cellDict):
        if len(cellDict) != 8:
            raise ValueError('Example requires eight S2 cells')
        for k in cellDict:
            self._set_value(KEY_S2_CELLS, LIST_TYPE_BYTES, cellDict[k], int(k))

    def s2_cell(self, level):
        cell = self._get_value(KEY_S2_CELLS, LIST_TYPE_BYTES, level)
        if cell == "":
            raise ValueError("S2Cell missing at level:", level)
        return cell

    def set_eco_region(self, realm, biome, num):
        if realm == "" or biome == "" or num == "":
            raise ValueError('Realm, Biome and EcoNum can not be empty')
        # self._set_value(KEY_ECO_REALM, LIST_TYPE_BYTES, realm)
        self._set_value(KEY_ECO_BIOME, LIST_TYPE_BYTES, biome)
        self._set_value(KEY_ECO_NUM, LIST_TYPE_BYTES, num)

    def coordinates(self):
        return self.latitude(), self.longitude()

    def elevation(self):
        return self._get_value(KEY_ELEVATION, LIST_TYPE_FLOAT)

    def set_elevation(self, elevation):
        self._set_value(KEY_ELEVATION, LIST_TYPE_FLOAT, elevation)

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

    # def set_random_location_values(self):
    #     import random
    #     from datetime import datetime
    #
    #     NORTHERNMOST = 49.
    #     SOUTHERNMOST = 25.
    #     EASTERNMOST = -66.
    #     WESTERNMOST = -124.
    #
    #     date = datetime(
    #         random.randint(2015, 2017),
    #         random.randint(1, 12),
    #         random.randint(1, 28)
    #     )
    #
    #     self.set_taxon(0)
    #     self.set_occurrence_id(str(random.randint(100000,900000)))
    #     self.set_longitude(round(random.uniform(EASTERNMOST, WESTERNMOST), 6))
    #     self.set_latitude(round(random.uniform(SOUTHERNMOST, NORTHERNMOST), 6))
    #     self.set_date(int(date.strftime("%s")))
        # while True:
        #     lat = round(random.uniform(self.SOUTHERNMOST, self.NORTHERNMOST), 6)
        #     lng = round(random.uniform(self.EASTERNMOST, self.WESTERNMOST), 6)
        #     try:
        #         gcode = Geocoder.reverse_geocode(lat, lng)
        #
        #         if gcode[0].data[0]['formatted_address'][-6:] in ('Canada', 'Mexico'):
        #             continue
        #         elif 'unnamed road' in gcode[0].data[0]['formatted_address']:
        #             continue
        #         elif 'Unnamed Road' in gcode[0].data[0]['formatted_address']:
        #             continue
        #         else:
        #             return gcode[0].coordinates[0], gcode[0].coordinates[1], date
        #     except GeocoderError:
        #         continue


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


# def RandomExample():
#     e = Example()
#     e.set_random_location_values()
#     return e


def FromSerialized(serialized):
    e = Example()
    e.decode_from_string(serialized)
    return e

def ParseExampleFromFirestore(category_id, example_id, o):
    e = Example()

    e.set_category(category_id)
    e.set_example_id(example_id)

    # This is a hack to avoid indexing the 'Date' property in Go.
    # 20170215: Should already be done.
    # if int(occurrence['FormattedDate']) < 19700101:
    #     continue
    e.set_date(o['FormattedDate'])

    e.set_latitude(o['GeoPoint']['latitude'])
    e.set_longitude(o['GeoPoint']['longitude'])

    if 'Elevation' not in o:
        raise ValueError('Elevation must be set in Firestore Occurrence')

    e.set_elevation(o['Elevation'])

    e.set_s2_cell(o['S2Tokens'])

    e.set_eco_region(o["EcoRealm"], o["EcoBiome"], o["EcoNum"])

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