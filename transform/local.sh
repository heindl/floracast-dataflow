#!/usr/bin/env bash

DATE=$(date '+%s')
TAXON=53713
TAXON_DATE=1513551459
RANDOM_DATE=1510706694

OCCURRENCE_PATH="floracast-datamining/occurrences/$TAXON/$TAXON_DATE"
RANDOM_PATH="floracast-datamining/random/$RANDOM_DATE"

mkdir -p "/tmp/$OCCURRENCE_PATH"
gsutil rsync -d -r "gs://$OCCURRENCE_PATH" "/tmp/$OCCURRENCE_PATH"
mkdir -p "/tmp/$RANDOM_PATH"
gsutil rsync -d -r "gs://$RANDOM_PATH" "/tmp/$RANDOM_PATH"

TRANSFORMED_PATH="/tmp/floracast-datamining/transformed/$TAXON/$DATE"

rm -rf "$TRANSFORMED_PATH"
mkdir -p

python ./main.py \
    --runner=DirectRunner \
    --job_name="floracast-transform" \
    --occurrence_location="/tmp/$OCCURRENCE_PATH" \
    --temp_location="/tmp/floracast-datamining/temp" \
    --random_location="/tmp/$RANDOM_PATH" \
    --output_location="$TRANSFORMED_PATH" \
    --mode "train" \
    --percent_eval 10 \
    --setup_file /Users/m/Desktop/phenograph/infra/src/bitbucket.org/heindl/dataflow/transform/setup.py \
    --workerLogLevelOverrides=com.google.cloud.dataflow#DEBUG