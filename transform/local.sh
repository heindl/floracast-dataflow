#!/usr/bin/env bash

DATE=$(date '+%s')
TAXON=58682

mkdir -p /tmp/floracast-datamining/occurrences/58682/1510696440
gsutil rsync -d -r gs://floracast-datamining/occurrences/58682/1510696440 /tmp/floracast-datamining/occurrences/58682/1510696440
mkdir -p /tmp/floracast-datamining/random/1510706694
gsutil rsync -d -r gs://floracast-datamining/random/1510706694 /tmp/floracast-datamining/random/1510706694

TRANSFORMED_PATH="/tmp/floracast-datamining/transformed/$TAXON/$DATE"

rm -rf "$TRANSFORMED_PATH"
mkdir -p

python ./main.py \
    --runner=DirectRunner \
    --job_name="floracast-transform" \
    --occurrence_location="/tmp/floracast-datamining/occurrences/58682/1510696440" \
    --temp_location="/tmp/floracast-datamining/temp" \
    --random_location="/tmp/floracast-datamining/random/1510706694" \
    --output_location="$TRANSFORMED_PATH" \
    --mode "train" \
    --percent_eval 10 \
    --setup_file /Users/m/Desktop/phenograph/infra/src/bitbucket.org/heindl/dataflow/transform/setup.py \
    --workerLogLevelOverrides=com.google.cloud.dataflow#DEBUG