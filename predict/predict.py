import tensorflow as tf
import io, json, os
import argparse
from google.cloud import storage, exceptions
import tempfile
from train_shared import train_shared_model
from fetch_shared import gcs, utils
from os import path
import sys
import shutil
import errno
from datetime import datetime
import functools
import re

def trim_path(gcs_path, bucket_name):
    s = "gs://%s/" % bucket_name
    if gcs_path.startswith(s):
        return gcs_path[len(s):]
    else:
        return gcs_path

def get_taxon_from_model_path(p):

    if p.endswith("/"):
        p = p[:-1]

    spl = p.split("/")

    return spl[len(spl) - 2]

def get_date_from_protected_area_path(p):

    if p.endswith("/"):
        p = p[:-1]

    spl = p.split("/")

    return spl[len(spl) - 2]

def get_timestamp_from_path(p):

    if p.endswith("/"):
        p = p[:-1]

    spl = p.split("/")

    return spl[len(spl) - 1]

def download_gcs_directory_to_temp(bucket, gcs_path):
    if gcs_path.startswith("gs://") is False:
        return gcs_path

    temp_file = tempfile.gettempdir()
    if temp_file == "" or temp_file is None:
        temp_file = "/tmp/"

    local_bucket = path.join(temp_file, bucket.id)
    local_path = path.join(local_bucket, trim_path(gcs_path, bucket.id))

    # Note that this is obviously very dangerous but for temp data should be ok.
    if os.path.exists(local_path):
        # less than ten is a sanity check
        if local_path == "" or local_path == "/" or temp_file not in local_path or len(local_path) < 10:
            raise ValueError('Invalid path to be deleted: %s', local_path)
        else:
            shutil.rmtree(local_path)

    os.makedirs(local_path)

    for blob in bucket.list_blobs(prefix=trim_path(gcs_path, bucket.id)):

        fname = path.join(local_bucket, blob.name)
        if fname.endswith("/"):
            try:
                os.makedirs(fname)
            except OSError as exc:  # Python >2.5
                if exc.errno == errno.EEXIST and os.path.isdir(fname):
                    pass
                else:
                    raise
            continue

        # Ensure the parent exists
        parent = "/".join(fname.split('/')[:-1]) + "/"
        try:
            os.makedirs(parent)
        except OSError as exc:  # Python >2.5
            if exc.errno == errno.EEXIST and os.path.isdir(parent):
                pass
            else:
                raise

        blob.download_to_filename(fname)

    return local_path

def get_latest_local(local_path):
    lst = os.listdir(local_path)
    lst.sort()
    return local_path + "/" + lst[-1]


def main(argv=None):

    parser = argparse.ArgumentParser()
    parser.add_argument('--bucket', type=str, required=True)
    parser.add_argument('--date', type=str, required=True)
    parser.add_argument('--taxon', type=str, required=True)
    args = parser.parse_args()

    _project = utils.default_project()

    if len(args.date_string) != 8:
        sys.exit("Date must be in format YYYYMMDD.")

    if len(args.taxon) == 0:
        sys.exit("A valid taxon is required.")

    # model_gcs = gcs.fetch_latest(_project, args.bucket, "models/%s" % args.taxon)
    # # Go another level down to get the export directory
    # # model_gcs = gcs.fetch_latest(_project, args.bucket, "%s/export/exporter" % model_gcs[len("gs://floracast-datamining/"):])
    #
    # transformed = gcs.fetch_latest(_project, args.bucket, "transformed/%s" % args.taxon)
    # protected_areas = gcs.fetch_latest(_project, args.bucket, "protected_areas/%s" % args.date)

    # print(model_gcs, transformed, protected_areas)

    # Download temp files
    # client = storage.client.Client(project=_project)
    # try:
    #     bucket = client.get_bucket(args.bucket)
    # except exceptions.NotFound:
    #     print('Sorry, that bucket does not exist!')
    #     return
    #
    # # Note that the timestamp for the model and transformed should be the same.
    # model_path = download_gcs_directory_to_temp(bucket, model_gcs)
    # transformed_path = download_gcs_directory_to_temp(bucket, transformed)
    # protected_area_path = download_gcs_directory_to_temp(bucket, protected_areas)

    model_path = get_latest_local(os.path.join("/tmp", args.bucket, "models", args.taxon))
    transformed_path = get_latest_local(os.path.join("/tmp", args.bucket, "transformed", args.taxon))
    protected_area_path = get_latest_local(os.path.join("/tmp", args.bucket, "protected_areas", args.date_string))

    print(model_path, transformed_path, protected_area_path)

    run_config = tf.estimator.RunConfig()
    run_config = run_config.replace(model_dir=model_path)

    args.train_data_path = transformed_path
    estimator = train_shared_model.get_estimator(args, run_config)

    # label_vocabulary = train_shared_model.get_label_vocabularly(transformed_path)

    values = []
    random_count = 0
    target_count = 1
    for _, _, files in os.walk(protected_area_path):
        for file in files:

            if "taxon-amended" not in file:
                # This is a hack to avoid spending more time on tranformer
                file = ensure_taxon(protected_area_path, file)

            for p in estimator.predict(input_fn=functools.partial(train_shared_model.transformed_input_fn,
                                                            transformed_location=transformed_path,
                                                            mode=tf.estimator.ModeKeys.PREDICT,
                                                               raw_data_file_pattern=os.path.join(protected_area_path, file))):

                random_point = p['probabilities'][0]
                target_point = p['probabilities'][1]
                if random_point > target_point:
                    random_count = random_count + 1
                else:
                    target_count = target_count + 1

                values.append("%s,%.8f,%.8f" % (p['occurrence_id'].replace("|", ","), target_point, random_point))

    print("count", args.date_string, random_count, target_count)

    csv_path = os.path.join("predictions", args.taxon, args.date_string)
    local_dir = os.path.join("/tmp", args.bucket, csv_path)
    csv_file = datetime.now().strftime("%s") + ".csv"

    if not os.path.exists(local_dir):
        os.makedirs(local_dir)

    with open(os.path.join(local_dir, csv_file), "w+") as f:
        f.write('\n'.join(values))
    # blob = storage.Blob(os.path.join(local_dir, csv_file), bucket)
    # blob.upload_from_string('\n'.join(values))

# TODO: Improve this by figuring out how to ammend metadata.
def ensure_taxon(tfrecords_path, tfrecord_filename) :

    original_path = path.join(tfrecords_path, tfrecord_filename)

    options = tf.python_io.TFRecordOptions(tf.python_io.TFRecordCompressionType.GZIP)

    new_file_path = path.join(tfrecords_path, "taxon-amended"+tfrecord_filename)

    open(new_file_path, 'a').close()

    # recordWriter = tf.python_io.TFRecordWriter(re.sub('\.gz$', '', new_file_path), options=options)
    recordWriter = tf.python_io.TFRecordWriter(new_file_path, options=options)

    for example in tf.python_io.tf_record_iterator(original_path, options=options):
        e = tf.train.Example.FromString(example)
        e.features.feature["taxon"].bytes_list.value.append('0')
        recordWriter.write(e.SerializeToString())

    recordWriter.close()

    os.remove(original_path)

    return new_file_path


if __name__ == '__main__':
    main()