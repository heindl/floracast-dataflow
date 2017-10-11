#!/usr/bin/env bash


if [ $1 == "local" ]; then

    python ./transformer/main.py \
         --runner DirectRunner \
         --project floracast-firestore \
         --mode "train" \
         --raw_data_location /Users/m/Desktop/phenograph/infra/src/bitbucket.org/heindl/dataflow/gs/floracast-models/occurrences/1505437167 \
         --train_location /Users/m/Desktop/phenograph/infra/src/bitbucket.org/heindl/dataflow/gs/floracast-models/transformed \
         --temp_location /Users/m/Desktop/phenograph/infra/src/bitbucket.org/heindl/dataflow/gs/floracast-models/temp \
         --job_name floracast_transform \
         --num_classes 2 \
         --percent_eval 10
fi

if [ $1 == "remote" ]; then

    python ./transformer/main.py \
        --runner=DataflowRunner \
        --job_name="floracast-transform-2" \
        --temp_location="gs://floracast-datamining/temp" \
        --staging_location="gs://floracast-datamining/staging" \
        --train_location="gs://floracast-datamining/transformed" \
        --raw_data_location="gs://floracast-datamining/occurrences/1505437167" \
        --num_classes 2 \
        --mode "train" \
        --percent_eval 10

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