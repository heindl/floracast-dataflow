# from __future__ import absolute_import
import apache_beam as beam
from example import Example

@beam.typehints.with_input_types(Example)
@beam.typehints.with_output_types(Example)
class ElevationBundleDoFn(beam.DoFn):
    def __init__(self, project):
        super(ElevationBundleDoFn, self).__init__()
        from google.cloud import storage, exceptions
        client = storage.client.Client(project=project)
        try:
            bucket = client.get_bucket('floracast-configuration')
        except exceptions.NotFound:
            print('Sorry, that bucket does not exist!')
            return

        blob = storage.Blob('dataflow.sh', bucket)
        content=blob.download_as_string()
        for line in content.split(b'\n'):
            if "FLORACAST_GOOGLE_MAPS_API_KEY" in line.decode("utf-8"):
                self.FLORACAST_GOOGLE_MAPS_API_KEY = line.decode("utf-8").split("=")[1]

        # Type features.Example
        self._buffer = []

    def start_bundle(self):
        del self._buffer[:]

    def finish_bundle(self):
        from apache_beam.utils.windowed_value import WindowedValue
        from apache_beam.transforms import window
        self._set_elevations()

        for b in self._buffer:
            if b.elevation() is not None:
               yield WindowedValue(b, -1, [window.GlobalWindow()])

        # Type features.Example
        self._buffer = []

    def isclose(self, a, b, rel_tol=1e-09, abs_tol=0.0):
        return abs(a-b) <= max(rel_tol * max(abs(a), abs(b)), abs_tol)

    def _set_elevations(self):
        from googlemaps import Client
        client = Client(key=self.FLORACAST_GOOGLE_MAPS_API_KEY)

        if len(self._buffer) == 0:
            return

        coords = []
        for e in self._buffer:
            if e.elevation() is None:
                coords.append(e.coordinates())

        if len(coords) == 0:
            return

        for r in client.elevation(coords):
            for i, b in enumerate(self._buffer):
                if self._buffer[i].elevation() is not None:
                    continue
                if not self.isclose(
                        r['location']['lat'],
                        self._buffer[i].latitude(),
                        abs_tol=0.00001
                ):
                    continue
                if not self.isclose(
                        r['location']['lng'],
                        self._buffer[i].longitude(),
                        abs_tol=0.00001
                ):
                    continue
                self._buffer[i].set_elevation(r['elevation'])
                break

    def process(self, e):

        if e.elevation() is not None:
            yield e
            return

        self._buffer.append(e)

        if len(self._buffer) >= 200:
            self._set_elevations()
            for e in self._buffer:
                if e.elevation() is not None:
                    yield e
            self._buffer = []