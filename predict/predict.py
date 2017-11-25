import tensorflow as tf
import io, json, os
import model
import argparse
from google.cloud import storage, exceptions
import tempfile
import path
from train_shared import input_fn

def trim_path(gcs_path, bucket_name):
    s = "gs://%s/" % bucket_name
    if gcs_path.startswith(s):
        return gcs_path[len(s)-1:]
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

    local_path = path.join(tempfile.gettempdir(), trim_path(gcs_path, bucket.id))
    os.makedirs(local_path)

    model_blob = storage.Blob(trim_path(gcs_path, bucket.id), bucket)
    model_blob.download_to_filename(local_path)

    return local_path

def main(argv=None):

    run_config = tf.contrib.learn.RunConfig()

    parser = argparse.ArgumentParser()
    parser.add_argument('--project', type=str, required=True)
    parser.add_argument('--bucket', type=str, required=True)

    parser.add_argument('--transformed', type=str, required=True)
    parser.add_argument('--output', type=str, required=True)
    parser.add_argument('--model', type=str, required=True)
    parser.add_argument("--protected_areas", type=str, required=True)
    args = parser.parse_args()

    # Download temp files
    client = storage.client.Client(project=args.project)
    try:
        bucket = client.get_bucket(args.bucket)
    except exceptions.NotFound:
        print('Sorry, that bucket does not exist!')
        return

    # Note that the timestamp for the model and transformed should be the same.
    model_path = download_gcs_directory_to_temp(bucket, args.model)
    transformed_path = download_gcs_directory_to_temp(bucket, args.transformed)
    protected_area_path = download_gcs_directory_to_temp(bucket, args.protected_areas)

    #
    taxon = get_taxon_from_model_path(model_path)

    #
    # if args.tranformed == "":
    #     # Get the last updated transform file.
    #     transformed_path = "gs://"+bucket.id+"/transformed/"+taxon
    #     bucket.list_blobs("/transformed/"+taxon)
    #     bucket.list_blobs(prefix='training_data/cats')


    run_config = run_config.replace(model_dir=model_path)

    estimator = model.get_estimator(args, run_config)

    label_vocabulary = model.get_label_vocabularly(transformed_path)
    date = get_date_from_protected_area_path(args.prote)

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

    output_path = path.join(
        args.output,
        taxon,
        date,
        get_timestamp_from_path(model_path),
        get_timestamp_from_path(transformed_path),
        ("%s.json" % get_timestamp_from_path(protected_area_path)))

    with io.open(output_path, 'w', encoding='utf-8') as f:
        f.write(unicode(json.dumps(values, f, ensure_ascii=False)))


if __name__ == '__main__':
    main()