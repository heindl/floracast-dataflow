#!/usr/bin/env bash

if [ $1 == "local" ]; then

    python ./fetcher/main.py \
         --runner DirectRunner \
         --project floracast-firestore \
         --mode "train" \
         --intermediate_location /Users/m/Desktop/phenograph/infra/src/bitbucket.org/heindl/dataflow/gs/floracast-models/occurrences \
         --train_location /Users/m/Desktop/phenograph/infra/src/bitbucket.org/heindl/dataflow/gs/floracast-models/train \
         --temp_location /Users/m/Desktop/phenograph/infra/src/bitbucket.org/heindl/dataflow/gs/floracast-models/temp \
         --infer_location /Users/m/Desktop/phenograph/infra/src/bitbucket.org/heindl/dataflow/gs/floracast-models/forests \
         --job_name floracast-fetch-occurrences \
         --occurrence_taxa 58682
#         --setup_file /Users/m/Desktop/phenograph/infra/src/bitbucket.org/heindl/dataflow/fetcher/setup.py
#         --requirements_file /Users/m/Desktop/phenograph/infra/src/bitbucket.org/heindl/dataflow/floracast_preprocess/requirements.txt
fi

if [ $1 == "remote" ]; then

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
        --setup
        _file /Users/m/Desktop/phenograph/infra/src/bitbucket.org/heindl/dataflow/fetcher/setup.py \
        --workerLogLevelOverrides=com.google.cloud.dataflow#DEBUG
#        --requirements_file /Users/m/Desktop/phenograph/infra/src/bitbucket.org/heindl/dataflow/floracast_preprocess/requirements.txt
      # --protected_area_count 3 \
fi