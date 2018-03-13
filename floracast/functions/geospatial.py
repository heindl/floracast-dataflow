from geopy import Point
from geopy.distance import vincenty

class GeospatialBounds:
    def __init__(self, lat=0, lng=0, radius_kilometers=0):
        self._center = Point(lat, lng)
        self._radius_kilometers = radius_kilometers

    def from_coordinates(self, n, e, s, w):
        ne = Point(n, e)
        sw = Point(s, w)
        radius = vincenty(sw, ne).kilometers / 2
        center = vincenty(kilometers=radius).destination(sw, 45)
        self._center = Point(center.latitude, center.longitude)
        self._radius_kilometers = radius

        return self

    def extend_radius(self, additional_kilometers=0):
        self._radius_kilometers = self._radius_kilometers + additional_kilometers

    def _from_center(self, bearing):
        return vincenty(kilometers=self._radius_kilometers).destination(self._center, bearing)

    def north(self):
        return self._from_center(0).latitude

    def south(self):
        return self._from_center(180).latitude

    def east(self):
        return self._from_center(90).longitude

    def west(self):
        return self._from_center(270).longitude