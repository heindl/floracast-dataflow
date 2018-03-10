#!/usr/bin/env bash

DATE=$(date '+%s')

python ./main.py \
    --runner=DataflowRunner \
    --job_name="floracast-transform" \
    --temp_location="gs://floracast-datamining/temp" \
    --staging_location="gs://floracast-datamining/staging" \
    --occurrence_file_pattern="gs://floracast-datamining/occurrences/58682/1510696440/*.gz" \
    --random_location="gs://floracast-datamining/random/1510706694" \
    --output_location="gs://floracast-datamining/transformed" \
#    --template_location="gs://floracast-datamining/templates/transform/$DATE" \
    --mode "train" \
    --percent_eval 10 \
    --setup_file /Users/m/Desktop/phenograph/infra/src/bitbucket.org/heindl/dataflow/transform/setup.py \
    --workerLogLevelOverrides=com.google.cloud.dataflow#DEBUG