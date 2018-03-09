# from __future__ import absolute_import

import apache_beam as beam
from google.cloud import storage, exceptions
from example import Examples
from datetime import datetime
from os import path, makedirs
import logging


# @beam.typehints.with_input_types(beam.typehints.KV[str, beam.typehints.List[Example]])
class WriteTFRecords(beam.DoFn):
    def __init__(self, project, dir, timestamp=None):
        super(WriteTFRecords, self).__init__()
        self._project = project
        self._directory = dir
        self._time = timestamp if timestamp is not None else datetime.now().strftime("%s")

    def _provided_directory(self):
        dr = self._directory.get() if hasattr(self._directory, 'get') else self._directory
        if dr is None or dr == "":
            raise ValueError("Invalid provided write directory:", self._provided_directory())
        return dr

    def _use_cloud_storage(self):
        return self._provided_directory().startswith("gs://")

    def _bucket_name(self):
        if not self._use_cloud_storage():
            raise ValueError("Can not parse bucket from invalid CloudStorage Path:", self._provided_directory())
        return self._provided_directory()[len("gs://"):].split("/")[0]

    def _cloud_storage_path(self, category):
        if not self._use_cloud_storage():
            return None
        return path.join(self._sub_path(category), self._file_name())

    def _sub_path(self, category):

        category = category.lower()

        if category == "" or "/" in category or " " in category:
            raise ValueError("Invalid Category")

        if "random" in category:
            return "random"
        elif "protectedarea" in category:
            return "protected_areas/"+category.split("-")[1]
        else:
            return "occurrences/%s" % category

    def _local_path(self, category):

        if self._provided_directory().strip("/").strip(" ") == "":
            raise ValueError("Invalid Directory")

        directory = self._provided_directory()
        if self._use_cloud_storage():
            directory = "/tmp/" + self._provided_directory()[len("gs://")-1:]

        directory = path.join(directory, self._sub_path(category))

        if not path.exists(directory):
            makedirs(directory)

        return path.join(directory, self._file_name())

    def _file_name(self):
        # return "%s.tfrecord.gz" % self._time
        return "%s.tfrecords" % self._time

    def _upload(self, category):
        cloud_path = self._cloud_storage_path(category)
        if cloud_path is not None:
            try:
                bucket = storage.Client(project=self._project).bucket(self._bucket_name())
            except exceptions.NotFound:
                logging.error('GCS Bucket [%s] does not exist', self._bucket_name())
                return
            record = storage.Blob(cloud_path, bucket)
            # There is a problem with gzip, so avoid using it.
            # The go reader requires Content-Encoding: gzip in order to read, however the
            # dataflow reader requires it to be there. So save as unencoded for now, though
            # the file is at least three times larger.
            # record.content_encoding = "gzip"
            record.upload_from_filename(self._local_path(category))

    def _write(self, category, examples):
        fpath = self._local_path(category)
        examples.write(fpath)
        return fpath

    def process(self, (category, iter)):
        examples = Examples(list(iter))
        _ = self._write(category, examples)
        self._upload(category)


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