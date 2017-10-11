#!/usr/bin/env bash

# TODO: Do not run this again without minimizing the number of weather records fetched.

if [ $1 == "local" ]; then

    python ./floracast_preprocess/main.py \
         --runner DirectRunner \
         --project floracast-firestore \
         --mode "infer" \
         --intermediate_location /Users/m/Desktop/phenograph/infra/src/bitbucket.org/heindl/dataflow/gs/floracast-models/occurrences \
         --train_location /Users/m/Desktop/phenograph/infra/src/bitbucket.org/heindl/dataflow/gs/floracast-models/train \
         --temp_location /Users/m/Desktop/phenograph/infra/src/bitbucket.org/heindl/dataflow/gs/floracast-models/temp \
         --infer_location /Users/m/Desktop/phenograph/infra/src/bitbucket.org/heindl/dataflow/gs/floracast-models/forests \
         --weeks_before 3 \
         --protected_area_count 3 \
         --job_name floracast-fetch-wilderness \
         --setup_file /Users/m/Desktop/phenograph/infra/src/bitbucket.org/heindl/dataflow/floracast_preprocess/setup.py
fi

if [ $1 == "remote" ]; then

    python ./floracast_preprocess/main.py \
        --runner=DataflowRunner \
        --temp_location="gs://floracast-datamining/temp" \
        --staging_location="gs://floracast-datamining/staging" \
        --train_location="gs://floracast-datamining/train" \
        --infer_location="gs://floracast-datamining/wilderness_areas" \
        --weeks_before 3 \
        --protected_area_count 3 \
        --job_name floracast-fetch-protected-areass \
        --mode "infer" \
        --setup_file /Users/m/Desktop/phenograph/infra/src/bitbucket.org/heindl/dataflow/floracast_preprocess/setup.py

fi