import tensorflow as tf
from random import randint, choice
from math import ceil
from example import Example
from string import ascii_letters, digits
from tensorflow.python.lib.io import tf_record
TFRecordCompressionType = tf_record.TFRecordCompressionType
from glob import iglob
import os
import errno
from google.cloud import storage

TEMP_DIRECTORY = "/tmp/"

class OccurrenceTFRecords:

    def __init__(self,
                 name_usage_id,
                 project,
                 gcs_bucket,
                 occurrence_path=None,
                 random_path=None,
                 multiplier_of_random_to_occurrences=5,
                 test_train_split_percentage=0.05,
        ):

        self._output_path = TEMP_DIRECTORY + "".join(choice(ascii_letters + digits) for x in range(randint(8, 12)))

        self._occurrence_path = occurrence_path if occurrence_path is not None else os.path.join(self._output_path, "occurrences/")
        self._random_path = random_path if random_path is not None else os.path.join(self._output_path, "random/")

        self._occurrence_file = os.path.join(self._occurrence_path, "occurrences.tfrecords")

        self._eval_output = os.path.join(self._output_path, "eval.tfrecords.gz")
        self._train_output = os.path.join(self._output_path, "train.tfrecords.gz")

        if not self._occurrence_path.startswith(TEMP_DIRECTORY) or not self._random_path.startswith(TEMP_DIRECTORY) or not self._output_path.startswith(TEMP_DIRECTORY):
            raise ValueError("Invalid TFRecords Output Path")

        try:
            os.makedirs(self._output_path)
        except OSError as exc:
            if exc.errno != errno.EEXIST:
                raise
            pass

        if occurrence_path is None:
            try:
                os.makedirs(self._occurrence_path)
            except OSError as exc:
                if exc.errno != errno.EEXIST:
                    raise
                pass
            self._client = storage.Client(project)
            self._gcs_bucket = self._client.get_bucket(gcs_bucket)
            self._fetch_occurrences(name_usage_id)

        self._occurrence_count = OccurrenceTFRecords.count(self._occurrence_file)

        if random_path is None:
            try:
                os.makedirs(self._random_path)
            except OSError as exc:
                if exc.errno != errno.EEXIST:
                    raise
                pass
            self._client = storage.Client(project)
            self._gcs_bucket = self._client.get_bucket(gcs_bucket)
            self._fetch_random()

        self._count_random_directory(test_train_split_percentage)

        total_random_count = multiplier_of_random_to_occurrences * self._occurrence_count


        self._eval_random_count = int(round(total_random_count * test_train_split_percentage))
        self._eval_occurrence_count = int(round(self._occurrence_count * test_train_split_percentage))

        self._train_random_count = total_random_count - self._eval_random_count
        self._train_occurrence_count = self._occurrence_count - self._eval_occurrence_count

    def __del__(self):
        # cleanup local occurrence data.
        if not self._output_path.startswith(TEMP_DIRECTORY):
            raise ValueError("Invalid TFRecords Output Path")
        # if os.path.isdir(self._output_path):
        #     shutil.rmtree(self._output_path)

    def _latest_gcs_occurrence_blob(self, name_usage):
        file_names = {} # Should be a list of [timestamp].tfrecords

        for b in self._gcs_bucket.list_blobs(prefix="occurrences/"+name_usage):
            _, f = os.path.split(b.name)
            file_names[f] = b
        if len(file_names) == 0:
            raise ValueError("Occurrence file not found for NameUsage", name_usage)
        k = sorted(file_names.keys(), reverse=True)[0]
        return file_names[k]


    def _fetch_occurrences(self, name_usage):
        if len(name_usage) == 0:
            raise ValueError("Invalid NameUsage")
        obj = self._latest_gcs_occurrence_blob(name_usage)
        obj.download_to_filename(self._occurrence_file)

    def _latest_random_path(self):
        dates = set() # Should be a list of [timestamp].tfrecords
        for blob in self._gcs_bucket.list_blobs(prefix="random/"):
            file_name = blob.name
            dates.add(file_name.split("/")[1])
        if len(dates) == 0:
            raise ValueError("Random path not found")
        return "random/%s" % sorted(list(dates), reverse=True)[0]

    def _count_random_directory(self, test_train_split_percentage):
        self._random_count = 0
        self._random_file_count = 0
        for f in iglob(self._random_path+"/*.tfrecords"):
            self._random_file_count += 1
            self._random_count = self._random_count + OccurrenceTFRecords.count(f)
        self._random_occurrences_per_file = int(round(self._random_count / self._random_file_count))
        self._random_eval_per_file = int(round((test_train_split_percentage * self._random_occurrences_per_file)))

    def _fetch_random(self):
        gcs_random_path = self._latest_random_path()
        i = 0
        while True:
            i += 1
            filename = ("%d.tfrecords" % i)
            local_file = os.path.join(self._random_path, filename)
            blob = self._gcs_bucket.get_blob(gcs_random_path + "/" + filename)
            if blob is None:
                return
            blob.download_to_filename(local_file)

    @staticmethod
    def is_occurrence(s):
        e = Example()
        e.decode_from_string(s)
        return e.category().lower() != "random"

    @staticmethod
    def count(filepath, compression_type=TFRecordCompressionType.NONE):
        options = tf_record.TFRecordOptions(compression_type)
        total = 0
        # occurrences = 0
        for _name in iglob(filepath):
            for e in tf.python_io.tf_record_iterator(_name, options=options):
                total += 1
                # if OccurrenceTFRecords.is_occurrence(e):
                #     occurrences += 1
        return total

    @staticmethod
    def get_first_id(filepath, compression_type=TFRecordCompressionType.NONE):
        for _name in iglob(filepath):
            for s in tf.python_io.tf_record_iterator(
                    _name,
                    options=tf_record.TFRecordOptions(compression_type)
            ):
                e = Example()
                e.decode_from_string(s)
                return e.example_id()


    def _generate_random_filenames(self):
        files = set()
        required_file_count = int(round((
                (self._train_random_count + self._eval_random_count) / self._random_occurrences_per_file)
        ))
        while len(files) <= required_file_count:
            i = randint(1, self._random_file_count-1)
            local_file = os.path.join(self._random_path, "%d.tfrecords" % i)
            files.add(local_file)
        return files

    @staticmethod
    def _generate_random_eval_positions(c, filesize):
        if c == 0:
            return None
        if filesize == 0:
            return None
        random_points = set()
        while len(random_points) < c:
            random_points.add(randint(0, filesize-1))
        return random_points

    def _prepare_eval_train_files(self):

        if not self._eval_output.startswith(TEMP_DIRECTORY) or not self._eval_output.startswith(TEMP_DIRECTORY):
            raise ValueError("Invalid Eval/Train Output")

        try:
            os.remove(self._eval_output)
        except OSError:
            pass
        open(self._eval_output,"w+").close()

        try:
            os.remove(self._train_output)
        except OSError:
            pass
        open(self._train_output,"w+").close()

    def train_test_split(self):

        self._prepare_eval_train_files()

        write_options = tf_record.TFRecordOptions(TFRecordCompressionType.GZIP)
        read_options = tf_record.TFRecordOptions(TFRecordCompressionType.NONE)

        eval_writer = tf.python_io.TFRecordWriter(self._eval_output, options=write_options)
        train_writer = tf.python_io.TFRecordWriter(self._train_output, options=write_options)

        occurrence_eval_positions = self._generate_random_eval_positions(
            self._eval_occurrence_count,
            self._occurrence_count,
        )
        for i, e in enumerate(tf.python_io.tf_record_iterator(self._occurrence_file, options=read_options)):
            if i in occurrence_eval_positions:
                eval_writer.write(e)
            else:
                train_writer.write(e)

        _eval_random_count = 0
        _train_random_count = 0
        for f in self._generate_random_filenames():
            file_size = OccurrenceTFRecords.count(f)
            remaining_eval_count = (self._eval_random_count - _eval_random_count)
            random_eval_positions = OccurrenceTFRecords._generate_random_eval_positions(
                (remaining_eval_count if remaining_eval_count < self._random_eval_per_file else self._random_eval_per_file),
                file_size,
            )
            for i, e in enumerate(tf.python_io.tf_record_iterator(f, options=read_options)):
                if random_eval_positions is not None and i in random_eval_positions:
                    _eval_random_count += 1
                    eval_writer.write(e)
                elif i not in random_eval_positions and _train_random_count < self._train_random_count:
                    _train_random_count += 1
                    train_writer.write(e)

        eval_writer.flush()
        train_writer.flush()

        eval_writer.close()
        train_writer.close()

        return self._eval_output, self._train_output