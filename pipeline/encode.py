from tensorflow.core.example import example_pb2
import apache_beam as beam

# Filter and prepare for duplicate sort.
@beam.typehints.with_input_types(example_pb2.SequenceExample)
@beam.typehints.with_output_types(str)
class EncodeExample(beam.DoFn):
    def __init__(self):
        super(EncodeExample, self).__init__()
        from apache_beam.metrics import Metrics
        self.final_occurrence_count = Metrics.counter('main', 'final_occurrence_count')

    def process(self, ex):
        if "elevation" not in ex.context.feature:
            return
        self.final_occurrence_count.inc()
        yield ex.SerializeToString()