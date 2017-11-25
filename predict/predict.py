import tensorflow as tf
import io, json, os
import argparse
from google.cloud import storage, exceptions
import tempfile
from train_shared import input_fn
from train_shared import model as train_shared_model
from fetch_shared import gcs, utils
from os import path
import sys
import shutil
import errno
from datetime import datetime

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

def main(argv=None):

    parser = argparse.ArgumentParser()
    parser.add_argument('--bucket', type=str, required=True)
    parser.add_argument('--date', type=str, required=True)
    parser.add_argument('--taxon', type=str, required=True)

    # parser.add_argument('--transformed', type=str, required=True)
    # parser.add_argument('--output', type=str, required=True)
    # parser.add_argument('--model', type=str, required=True)
    # parser.add_argument("--protected_areas", type=str, required=True)
    args = parser.parse_args()

    _project = utils.default_project()

    if len(args.date) != 8:
        sys.exit("Date must be in format YYYYMMDD.")

    if len(args.taxon) == 0:
        sys.exit("A valid taxon is required.")

    model_gcs = gcs.fetch_latest(_project, args.bucket, "models/%s" % args.taxon)
    # Go another level down to get the export directory
    model_gcs = gcs.fetch_latest(_project, args.bucket, "%s/export/exporter" % model_gcs[len("gs://floracast-datamining/"):])

    transformed = gcs.fetch_latest(_project, args.bucket, "transformed/%s" % args.taxon)
    protected_areas = gcs.fetch_latest(_project, args.bucket, "protected_areas/%s" % args.date)


    print(model_gcs, transformed, protected_areas)

    # return

    # Download temp files
    client = storage.client.Client(project=_project)
    try:
        bucket = client.get_bucket(args.bucket)
    except exceptions.NotFound:
        print('Sorry, that bucket does not exist!')
        return

    # Note that the timestamp for the model and transformed should be the same.
    model_path = download_gcs_directory_to_temp(bucket, model_gcs)
    transformed_path = download_gcs_directory_to_temp(bucket, transformed)
    protected_area_path = download_gcs_directory_to_temp(bucket, protected_areas)


    #
    # if args.tranformed == "":
    #     # Get the last updated transform file.
    #     transformed_path = "gs://"+bucket.id+"/transformed/"+taxon
    #     bucket.list_blobs("/transformed/"+taxon)
    #     bucket.list_blobs(prefix='training_data/cats')

    run_config = tf.estimator.RunConfig()
    run_config = run_config.replace(model_dir=model_path)

    args.train_data_path = transformed_path
    estimator = train_shared_model.get_estimator(args, run_config)

    label_vocabulary = train_shared_model.get_label_vocabularly(transformed_path)

    values = []
    for _, _, files in os.walk(protected_area_path):
        for file in files:

            # print(file)
            # date = file.split("-")[0]
            # print(date)
            # if date not in map:
            #     map[date] = []
        # date = dir
        # print(file)

            for p in estimator.predict(input_fn=input_fn.get_transformed_prediction_features(transformed_path, os.path.join(protected_area_path, file))):

                # print("p", p)

                probabilities = []

                for prob in p['probabilities']:
                    probabilities.append(float(prob))

                values.append({'probabilities': probabilities, 'classes': label_vocabulary, 'key': p['occurrence_id']})


    # for key in map:
    #
    #     if len(map[key]) == 0:
    #         continue

    prefix = "/predictions/%s/%s/%s.json" % (args.taxon, args.date, datetime.now().strftime("%s"))

    blob = storage.Blob(prefix, bucket)
    blob.upload_from_string(json.dumps(values))

    # output_path = path.join(
    #     args.output,
    #     args.taxon,
    #     date,
    #     "%s.json" % datetime.now().strftime("%s"))
    #
    # with io.open(output_path, 'w', encoding='utf-8') as f:
    #     f.write(unicode(json.dumps(values, f, ensure_ascii=False)))


if __name__ == '__main__':
    main()