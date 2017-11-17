#!/usr/bin/env bash


if [ $1 == "local" ]; then

    python ./transformer/main.py \
         --runner DirectRunner \
         --project floracast-firestore \
         --mode "train" \
         --raw_location /Users/m/Desktop/phenograph/infra/src/bitbucket.org/heindl/dataflow/gs/floracast-models/occurrences/58682/1510696440 \
         --random_location /Users/m/Desktop/phenograph/infra/src/bitbucket.org/heindl/dataflow/gs/floracast-models/random/1510706694 \
         --train_location /Users/m/Desktop/phenograph/infra/src/bitbucket.org/heindl/dataflow/gs/floracast-models/transformed/58682 \
         --temp_location /Users/m/Desktop/phenograph/infra/src/bitbucket.org/heindl/dataflow/gs/floracast-models/temp \
         --job_name floracast_transform
fi

if [ $1 == "remote" ]; then

    python ./transformer/main.py \
        --runner=DataflowRunner \
        --job_name="floracast-transform" \
        --temp_location="gs://floracast-datamining/temp" \
        --staging_location="gs://floracast-datamining/staging" \
        --raw_location="gs://floracast-datamining/occurrences/58682/1510696440" \
        --random_location="gs://floracast-datamining/random/1510706694" \
        --train_location="gs://floracast-datamining/transformed/58682" \
        --mode "train" \
        --percent_eval 10 \
        --setup_file /Users/m/Desktop/phenograph/infra/src/bitbucket.org/heindl/dataflow/transformer/setup.py \
        --workerLogLevelOverrides=com.google.cloud.dataflow#DEBUG

fi

#gcloud ml-engine local predict \
#    --model-dir ${YOUR_LOCAL_EXPORT_DIR}/saved_model --json-instances=inputs.json
#
#
#gcloud ml-engine jobs submit prediction "${JOB_ID}" \
#    --model "movielens" \
#    --input-paths "${GCS_PREDICTION_FILE}" \
#    --output-path "${GCS_PATH}/prediction/${JOB_ID}"\
#    --region us-central1 \
#    --data-format TF_RECORD_GZIP