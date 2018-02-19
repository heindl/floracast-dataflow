# from __future__ import absolute_import

import apache_beam as beam
from google.cloud import storage, exceptions
from os import path
from example import Example
from tensorflow import python_io
from tempfile import TemporaryFile
from datetime import datetime
from os import path


@beam.typehints.with_input_types(beam.typehints.KV[str, beam.typehints.Iterable[Example]])
class WriteTFRecords(beam.DoFn):
    def __init__(self, project, filepath):
        super(WriteTFRecords, self).__init__()
        self._project = project,
        self._filepath = filepath

    def process(self, (category, examples)):

        suffix = datetime.now().strftime("%s") + ".tfrecords"

        isRandom = "random" in category.lower()
        isProtectedArea = "protectedarea" in category.lower()
        isOccurrence = isRandom is False and isProtectedArea is False

        if isRandom:
            suffix = "/random/" + suffix
        elif isProtectedArea:
            suffix = "/protected_areas/" + suffix
        elif isOccurrence:
            suffix = "/occurrences/" + suffix

        isBoundForCloudStorage = self._filepath.startswith("gs://")

        if isBoundForCloudStorage is False:
            local_file = path.join(self._filepath, suffix)
        else:
            local_file = TemporaryFile()

        record_writer = python_io.TFRecordWriter(path=local_file, options=python_io.TFRecordOptions(python_io.TFRecordCompressionType.GZIP))

        for e in examples:
            record_writer.write(e)

        if isBoundForCloudStorage:
            bucket_name = self._filepath[len("gs://")-1:].split("/")[0]
            bucket = storage.Client(project=self._project).bucket(bucket_name)
            storage.Blob(suffix, bucket).upload_from_file(local_file)


def fetch_latest(project, bucket_name, parent_path):

    if parent_path.startswith("/"):
        parent_path = parent_path[1:]

    if parent_path.endswith("/"):
        parent_path = parent_path[:-1]

    client = storage.client.Client(project=project)
    try:
        bucket = client.get_bucket(bucket_name)
    except exceptions.NotFound:
        print('Sorry, that bucket does not exist!')
        return
    names = []
    # Note that paging should be done behind the scenes.
    # https://stackoverflow.com/questions/43147339/how-does-paging-work-in-the-list-blobs-function-in-google-cloud-storage-python-c
    for blob in bucket.list_blobs(prefix=parent_path, fields="items/name"):
        # print("name", blob.name)
        if blob.name == parent_path:
            continue
        name = blob.name[len(parent_path)+1:]
        s = name.split("/")
        names.append(path.join(parent_path, s[0]))

    names = sorted(names)

    if len(names) == 0:
        raise ValueError('No latest version found within folder on gcs with parent: %s', parent_path)

    return path.join("gs://", bucket_name, names[len(names)-1]).encode('utf-8')