#!/usr/bin/env bash

# TODO: Do not run this again without minimizing the number of weather records fetched.

if [ $1 == "local" ]; then

    python ../fetcher/main.py \
         --runner DirectRunner \
         --project floracast-firestore \
         --mode "infer" \
         --intermediate_location /Users/m/Desktop/phenograph/infra/src/bitbucket.org/heindl/dataflow/gs/floracast-models/occurrences \
         --train_location /Users/m/Desktop/phenograph/infra/src/bitbucket.org/heindl/dataflow/gs/floracast-models/train \
         --temp_location /Users/m/Desktop/phenograph/infra/src/bitbucket.org/heindl/dataflow/gs/floracast-models/temp \
         --infer_location /Users/m/Desktop/phenograph/infra/src/bitbucket.org/heindl/dataflow/gs/floracast-models/forests \
         --weeks_before 3 \
         --job_name floracast-fetch-wilderness \
         --setup_file /Users/m/Desktop/phenograph/infra/src/bitbucket.org/heindl/dataflow/fetcher/setup.py \
         --protected_area_count 500 \
#         --requirements_file /Users/m/Desktop/phenograph/infra/src/bitbucket.org/heindl/dataflow/floracast_preprocess/requirements.txt
fi

if [ $1 == "remote" ]; then

    python ../fetcher/main.py \
        --runner=DataflowRunner \
        --project floracast-firestore \
        --temp_location="gs://floracast-datamining/temp" \
        --staging_location="gs://floracast-datamining/staging" \
        --job_name floracast-fetch-protected-areas \
        --mode "infer" \
        --setup_file /Users/m/Desktop/phenograph/infra/src/bitbucket.org/heindl/dataflow/fetcher/setup.py \
        --workerLogLevelOverrides=com.google.cloud.dataflow#DEBUG
#        --requirements_file /Users/m/Desktop/phenograph/infra/src/bitbucket.org/heindl/dataflow/floracast_preprocess/requirements.txt
      # --protected_area_count 3 \
fi