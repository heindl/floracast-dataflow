#!/usr/bin/env bash

python ./fetcher/main.py \
        --runner=DataflowRunner \
        --temp_location="gs://floracast-datamining/temp" \
        --staging_location="gs://floracast-datamining/staging" \
        --train_location="gs://floracast-datamining/train" \
        --infer_location="gs://floracast-datamining/wilderness_areas" \
        --intermediate_location="gs://floracast-datamining/occurrences" \
        --occurrence_taxa 58682 \
        --job_name floracast-fetch-occurrences \
        --mode "train" \
        --setup_file /Users/m/Desktop/phenograph/infra/src/bitbucket.org/heindl/dataflow/fetcher/setup.py \
        --workerLogLevelOverrides=com.google.cloud.dataflow#DEBUG