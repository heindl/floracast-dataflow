# from __future__ import absolute_import
import apache_beam as beam
from example import Example
# from google.cloud.proto.datastore.v1 import entity_pb2
from apache_beam.transforms.core import PTransform
from apache_beam.io import iobase


def fetch_random(
        pipeline_options,
        output_path,
):
    import apache_beam as beam
    import weather as weather
    import elevation as elevation
    from apache_beam.io import WriteToText
    from apache_beam import pvalue
    import utils as utils
    from tensorflow_transform.beam import impl as tft

    options = pipeline_options.get_all_options()

    with beam.Pipeline(options['runner'], options=pipeline_options) as pipeline:
        with tft.Context(temp_dir=options['temp_location']):

            occurrences = pipeline \
                          | 'ReadRandomOccurrences' >> _ReadRandomOccurrences(count=options['random_occurrence_count']) \
                          | 'RemoveOccurrenceExampleLocationDuplicates' >> utils.RemoveOccurrenceExampleLocationDuplicates() \
                          | 'GroupByYearMonth' >> utils.GroupByYearMonth() \
                          | 'FetchWeather' >> beam.ParDo(weather.FetchWeatherDoFn(options['project'], options['weather_station_distance'])) \
                          | 'EnsureElevation' >> beam.ParDo(elevation.ElevationBundleDoFn(options['project'])) \
                          | 'ShuffleOccurrences' >> utils.Shuffle() \
                          | 'ProtoForWrite' >> beam.Map(lambda e: e.encode())

            _ = occurrences \
                | 'WriteRandomOccurrences' >> beam.io.WriteToTFRecord(output_path + "/", file_name_suffix='.tfrecord.gz')

            # _ = data \
            #     | 'EncodePredictAsB64Json' >> beam.Map(utils.encode_as_b64_json) \
            #     | 'WritePredictDataAsText' >> beam.io.WriteToText(output_path, file_name_suffix='.txt')

            # Write metadata
            _ = pipeline | beam.Create([{
                'weather_station_distance': options['weather_station_distance'],
                'random_occurrence_count': options['random_occurrence_count']
            }]) \
                | 'WriteToMetadataFile' >> WriteToText(output_path + "/", file_name_suffix=".query.meta", num_shards=1)


class ComputeWordLengths(beam.PTransform):
    def expand(self, pcoll):
        # transform logic goes here
        return pcoll | beam.Map(lambda x: len(x))

@beam.typehints.with_input_types(int)
@beam.typehints.with_output_types(Example)
class _AddRandomTrainPoints(beam.DoFn):
    def __init__(self):
        super(_AddRandomTrainPoints, self).__init__()

    def process(self, total_occurrence_count):
        import example as example
        import math
        if total_occurrence_count == 0:
            return
        random_example_count = total_occurrence_count * 0.20
        if random_example_count < 700:
            if total_occurrence_count <= 700:
                random_example_count = total_occurrence_count
            else:
                random_example_count = 700
        for _ in range(0, int(math.floor(random_example_count))):
            yield example.RandomExample()


class _RandomOccurrenceSource(iobase.BoundedSource):

    def __init__(self, count=0):
        # from apache_beam.metrics import Metrics
        # self.records_read = Metrics.counter(self.__class__, 'recordsRead')
        self._count = count

    def estimate_size(self):
        return 0

    def get_range_tracker(self, start_position, stop_position):
        from apache_beam.io import range_trackers
        if start_position is None:
            start_position = 0
        if stop_position is None:
            stop_position = range_trackers.OffsetRangeTracker.OFFSET_INFINITY

        # Use an unsplittable range tracker. This means that a collection can
        # only be read sequentially for now.
        range_tracker = range_trackers.OffsetRangeTracker(start_position, stop_position)
        range_tracker = range_trackers.UnsplittableRangeTracker(range_tracker)

        return range_tracker

    def read(self, range_tracker):
        import example as example

        for _ in range(0, self._count):
            yield example.RandomExample()

    def split(self, desired_bundle_size, start_position=None, stop_position=None):
        """Implements :class:`~apache_beam.io.iobase.BoundedSource.split`
        This function will currently not be called, because the range tracker
        is unsplittable
        """
        from apache_beam.io import range_trackers

        if start_position is None:
            start_position = 0
        if stop_position is None:
            stop_position = range_trackers.OffsetRangeTracker.OFFSET_INFINITY

        # Because the source is unsplittable (for now), only a single source is
        # returned.
        yield iobase.SourceBundle(
            weight=1,
            source=self,
            start_position=start_position,
            stop_position=stop_position)

class _ReadRandomOccurrences(PTransform):
    """A :class:`~apache_beam.transforms.ptransform.PTransform` for reading
    from MongoDB.
    """
    def __init__(self, count):
        """Initializes :class:`ReadFromMongo`
        Uses source :class:`_MongoSource`
        """
        super(_ReadRandomOccurrences, self).__init__()
        self._source = _RandomOccurrenceSource(count=count)

    def expand(self, pcoll):
        """Implements :class:`~apache_beam.transforms.ptransform.PTransform.expand`"""
        return pcoll | iobase.Read(self._source)

        # def display_data(self):
        #     return {'source_dd': self._source}