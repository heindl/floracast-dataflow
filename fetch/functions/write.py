# from __future__ import absolute_import

import apache_beam as beam
from google.cloud import storage, exceptions
from example import Examples
from datetime import datetime
from os import path, makedirs
import logging
import random
import string
import shutil

class Category:
    def __init__(self, cat):
        self._cat = cat
        if not self.valid():
            raise ValueError("Invalid Category", cat)

    def file_path(self, ts):

        if self.is_random():
            if not self.id():
                return None
            return "random/%s/%s.tfrecords" % (ts, self.id())

        if self.is_protected_area():
            if not self.id():
                return None
            return "protected_areas/%s/%s.tfrecords" % (self.id(), ts)

        if self.is_occurrence():
            return "occurrences/%s/%s.tfrecords" % (self._cat, ts)

    def valid(self):
        if not self._cat or "/" in self._cat or " " in self._cat:
            return False
        if (self.is_random() or self.is_protected_area()) and "-" not in self._cat:
            return False
        return True

    def is_random(self):
        return "random" in self._cat.lower()

    def is_protected_area(self):
        return "protected_area" in self._cat.lower()

    def id(self):
        if not self.is_random() and not self.is_protected_area():
            return None
        return self._cat.split("-")[1]

    def is_occurrence(self):
        return not self.is_random() and not self.is_occurrence()


# @beam.typehints.with_input_types(beam.typehints.KV[str, beam.typehints.List[Example]])
class ExampleRecordWriter(beam.DoFn):

    _TEMP_DIR = "/tmp/"

    def __init__(self, project, bucket=None, timestamp=None):
        super(ExampleRecordWriter, self).__init__()
        self._project_addr = project
        self._bucket_addr = bucket
        self._ts = timestamp if timestamp is not None else datetime.now().strftime("%s")

        self._local_dir = self._TEMP_DIR + "".join(random.choice(string.ascii_letters + string.digits) for x in range(random.randint(8, 12)))

    def __del__(self):
        if not self._local_dir.startswith(self._TEMP_DIR):
            raise ValueError("Invalid TFRecords Output Path")
        shutil.rmtree(self._local_dir)

    def _cloud_storage_path(self, cat):
        if not self._bucket:
            return None
        return path.join(self._bucket, cat.file_path(self._ts))

    def _local_path(self, cat):
        f = path.join(self._local_dir, cat.file_path(self._ts))
        if not f.startswith(self._TEMP_DIR):
            return None
        dir_struct, _ = path.split(f)
        if not path.exists(dir_struct):
            makedirs(dir_struct)
        return f

    def _upload(self, cat):
        cloud_path = self._cloud_storage_path(cat)
        local_path = self._local_path(cat)
        if cloud_path is not None and local_path is not None:
            try:
                bucket = storage.Client(project=self._project).bucket(self._bucket)
            except exceptions.NotFound:
                logging.error('GCS Bucket [%s] does not exist', self._bucket)
                return

            # There is a problem with gzip, so avoid using it.
            # The go reader requires Content-Encoding: gzip in order to read, however the
            # dataflow reader requires it to be there. So save as unencoded for now, though
            # the file is at least three times larger.
            # record.content_encoding = "gzip"
            storage.Blob(cloud_path, bucket).upload_from_filename(local_path)

    def process(self, (category, iter)):
        self._project = self._project_addr.get() if hasattr(self._project_addr, 'get') else self._project_addr
        self._bucket = self._bucket_addr.get() if hasattr(self._bucket_addr, 'get') else self._bucket_addr
        examples = Examples(list(iter))
        cat = Category(category)
        examples.write(self._local_path(cat))
        self._upload(cat)


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