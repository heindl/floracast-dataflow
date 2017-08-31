from tensorflow.core.example import example_pb2
import apache_beam as beam

@beam.typehints.with_input_types(example_pb2.SequenceExample)
@beam.typehints.with_output_types(example_pb2.SequenceExample)
class ElevationBundleDoFn(beam.DoFn):
    def __init__(self, project):
        super(ElevationBundleDoFn, self).__init__()
        from google.cloud import storage

        client = storage.Client(project=project)
        bucket = client.get_bucket('floracast-conf')
        content = storage.Blob('dataflow.sh', bucket).download_as_string()
        for line in content.split(b'\n'):
            if "FLORACAST_GOOGLE_MAPS_API_KEY" in line.decode("utf-8"):
                self.FLORACAST_GOOGLE_MAPS_API_KEY = line.decode("utf-8").split("=")[1]

        self._buffer = []

    def start_bundle(self):
        del self._buffer[:]

    def finish_bundle(self):
        from apache_beam.utils.windowed_value import WindowedValue
        from apache_beam.transforms import window
        self._set_elevations()

        print("finishing elevations", len(self._buffer))
        for b in self._buffer:
            yield WindowedValue(b, -1, [window.GlobalWindow()])

        self._buffer = []

    def isclose(self, a, b, rel_tol=1e-09, abs_tol=0.0):
        return abs(a-b) <= max(rel_tol * max(abs(a), abs(b)), abs_tol)

    def _set_elevations(self):
        from googlemaps import Client
        client = Client(key=self.FLORACAST_GOOGLE_MAPS_API_KEY)

        if len(self._buffer) == 0:
            return

        print("setting elevations", len(self._buffer))

        coords = []
        for e in self._buffer:
            coords.append((
                e.context.feature["latitude"].float_list.value[0],
                e.context.feature["longitude"].float_list.value[0]
            ))

        for r in client.elevation(coords):
            for i, b in enumerate(self._buffer):
                if len(self._buffer[i].context.feature["elevation"].float_list.value) > 0:
                    continue
                if not self.isclose(
                        r['location']['lat'],
                        b.context.feature['latitude'].float_list.value[0],
                        abs_tol=0.00001
                ):
                    continue
                if not self.isclose(
                        r['location']['lng'],
                        b.context.feature['longitude'].float_list.value[0],
                        abs_tol=0.00001
                ):
                    continue
                self._buffer[i].context.feature["elevation"].float_list.value.append(r['elevation'])
                break

    def process(self, element):

        if "elevation" in element.context.feature:
            yield element
            return

        self._buffer.append(element)

        if len(self._buffer) >= 200:
            self._set_elevations()
            print("processing elevations", len(self._buffer))
            for b in self._buffer:
                yield b
            self._buffer = []