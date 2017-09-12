import apache_beam as beam


# # TODO: Perhaps use Reshuffle (https://issues.apache.org/jira/browse/BEAM-1872)?
@beam.ptransform_fn
def _Shuffle(pcoll):  # pylint: disable=invalid-name
    import random
    return (pcoll
            | 'PairWithRandom' >> beam.Map(lambda x: (random.random(), x))
            | 'GroupByRandom' >> beam.GroupByKey()
            | 'DropRandom' >> beam.FlatMap(lambda (k, vs): vs))


def _encode_as_b64_json(serialized_example):
    import base64  # pylint: disable=g-import-not-at-top
    import json  # pylint: disable=g-import-not-at-top
    return json.dumps({'b64': base64.b64encode(serialized_example)})


def preprocess_infer(
        pipeline,
        project,
        data_path,
        metadata_path,
        weeks_before,
        weather_station_distance
    ):
    from tensorflow.contrib.learn import ModeKeys
    from tensorflow_transform import coders
    import forests, elevation, weather, example
    from tensorflow_transform.beam import tft_beam_io
    from tensorflow_transform.tf_metadata import dataset_metadata

    infer_schema = example.make_input_schema(mode=ModeKeys.INFER)
    infer_metadata = dataset_metadata.DatasetMetadata(schema=infer_schema)
    infer_coder = coders.ExampleProtoCoder(infer_schema)

    serialized_examples = pipeline \
                          | 'ReadDatastoreForests' >> forests.ReadDatastoreForests(project=project) \
                          | 'ConvertForestEntityToSequenceExample' >> beam.ParDo(
                                forests.ForestEntityToExample(periods=weeks_before)) \
                          | 'EnsureElevation' >> beam.ParDo(elevation.ElevationBundleDoFn(project)) \
                          | 'FetchWeather' >> beam.ParDo(weather.FetchWeatherDoFn(project, weather_station_distance)) \
                          | 'EncodePredictData' >> beam.Map(lambda e: infer_coder.encode(e.as_pb2()))

    _ = serialized_examples \
        | 'WritePredictDataAsTFRecord' >> beam.io.WriteToTFRecord(data_path, file_name_suffix='.tfrecord.gz')

    _ = serialized_examples \
        | 'EncodePredictAsB64Json' >> beam.Map(_encode_as_b64_json) \
        | 'WritePredictDataAsText' >> beam.io.WriteToText(data_path, file_name_suffix='.txt')

    _ = infer_metadata \
        | 'WriteInputMetadata' >> tft_beam_io.WriteMetadata(path=metadata_path, pipeline=pipeline)


def fetch_train(
        pipeline,
        records_file_path,
        mode,
        project,
        occurrence_taxa=None,
        weather_station_distance=75,
        minimum_occurrences_within_taxon=40,
        add_random_train_point=True
    ):
    import apache_beam as beam
    import occurrences, example, elevation, weather
    from apache_beam.io import WriteToText

    data = pipeline \
           | occurrences.ReadDatastoreOccurrences(project=project) \
        | 'ConvertEntitiesToKeyStrings' >> beam.ParDo(occurrences.OccurrenceEntityToExample(occurrence_taxa)) \
        | 'RemoveOccurrenceExampleLocationDuplicates' >> occurrences.RemoveOccurrenceExampleLocationDuplicates() \
        | 'RemoveScantTaxa' >> occurrences.RemoveScantTaxa(minimum_occurrences_within_taxon)

    if add_random_train_point is True:
        data = data | 'AddRandomTrainPoint' >> beam.FlatMap(lambda e: [e, example.RandomExample()])

    data = data \
        | 'EnsureElevation' >> beam.ParDo(elevation.ElevationBundleDoFn(project)) \
        | 'FetchWeather' >> beam.ParDo(weather.FetchWeatherDoFn(project, weather_station_distance)) \
        | 'ShuffleOccurrences' >> _Shuffle() \
        | 'ProtoForWrite' >> beam.Map(lambda e: e.encode())

    # schema = example.make_input_schema(mode)
    # proto_coder = coders.ExampleProtoCoder(schema)
    #
    # data = data | 'EncodeForWrite' >> beam.Map(proto_coder.encode)

    _ = data \
        | 'Write' >> beam.io.WriteToTFRecord(records_file_path, file_name_suffix='.tfrecord.gz')

    _ = data \
        | 'EncodePredictAsB64Json' >> beam.Map(_encode_as_b64_json) \
        | 'WritePredictDataAsText' >> beam.io.WriteToText(records_file_path, file_name_suffix='.txt')


    # Write metadata
    _ = beam.Create([{
                'taxa': occurrence_taxa,
                'weather_station_distance': weather_station_distance,
                'minimum_occurrences_within_taxon': minimum_occurrences_within_taxon,
                'random_train_points': add_random_train_point
            }]) \
        | 'WriteToMetadataFile' >> WriteToText(records_file_path, file_name_suffix=".meta")


def preprocess_train(
        pipeline,
        intermediate_records,
        mode,
        output_path,
        partition_random_seed,
        percent_eval,
        num_classes,
):
    import example
    from tensorflow_transform.beam import tft_beam_io
    from tensorflow_transform.beam import impl as tft
    from tensorflow_transform import coders
    from tensorflow_transform.tf_metadata import dataset_metadata
    import os

    RAW_METADATA_DIR = 'raw_metadata'
    TRANSFORMED_TRAIN_DATA_FILE_PREFIX = 'features_train'
    TRANSFORMED_EVAL_DATA_FILE_PREFIX = 'features_eval'

    input_schema = example.make_input_schema(mode)
    input_coder = coders.ExampleProtoCoder(input_schema)

    records = pipeline \
        | 'ReadInitialTFRecords' >> beam.io.ReadFromTFRecord(intermediate_records) \
        | 'DecodeProtoExamples' >> beam.Map(input_coder.decode)

    # records = records | 'RecordsDataset' >> beam.ParDo(occurrences.Counter("main"))

    preprocessing_fn = example.make_preprocessing_fn(num_classes)
    metadata = dataset_metadata.DatasetMetadata(schema=input_schema)

    _ = metadata \
        | 'WriteInputMetadata' >> tft_beam_io.WriteMetadata(
            path=os.path.join(output_path, RAW_METADATA_DIR),
            pipeline=pipeline)

    (records_dataset, records_metadata), transform_fn = (
        (records, metadata) | tft.AnalyzeAndTransformDataset(preprocessing_fn))

    _ = (transform_fn
         | 'WriteTransformFn' >> tft_beam_io.WriteTransformFn(output_path))

    def train_eval_partition_fn(ex, unused_num_partitions):
        return partition_fn(ex, partition_random_seed, percent_eval)

    train_dataset, eval_dataset = records_dataset \
        | 'TrainEvalPartition' >> beam.Partition(train_eval_partition_fn, 2)

    coder = coders.ExampleProtoCoder(records_metadata.schema)
    _ = train_dataset \
        | 'SerializeTrainExamples' >> beam.Map(coder.encode) \
        | 'ShuffleTraining' >> _Shuffle() \
        | 'WriteTraining' >> beam.io.WriteToTFRecord(
                os.path.join(output_path, TRANSFORMED_TRAIN_DATA_FILE_PREFIX),
                file_name_suffix='.tfrecord.gz')

    _ = eval_dataset \
        | 'SerializeEvalExamples' >> beam.Map(coder.encode) \
        | 'ShuffleEval' >> _Shuffle() \
        | 'WriteEval' >> beam.io.WriteToTFRecord(
                os.path.join(output_path, TRANSFORMED_EVAL_DATA_FILE_PREFIX),
                file_name_suffix='.tfrecord.gz')

    _ = train_dataset \
                  | 'CountTraining' >> beam.combiners.Count.Globally() \
                  | 'WriteTrainCount' >> beam.io.WriteToText(
                    os.path.join(output_path, 'train_count'), file_name_suffix=".txt")

    _ = eval_dataset \
        | 'CountEval' >> beam.combiners.Count.Globally() \
        | 'WriteEvalCount' >> beam.io.WriteToText(
                os.path.join(output_path, 'eval_count'), file_name_suffix=".txt")


# def partition_fn(user_id, partition_random_seed, percent_eval):
# I hope taxon_id will provide wide enough variation between results.
def partition_fn(ex, partition_random_seed, percent_eval):
    import hashlib
    m = hashlib.md5(str(ex["occurrence_id"][0] + partition_random_seed))
    hash_value = int(m.hexdigest(), 16) % 100
    return 0 if hash_value >= percent_eval else 1