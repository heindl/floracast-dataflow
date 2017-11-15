#!/usr/bin/env bash

if [ $1 == "local" ]; then

    python ./fetcher/main.py \
         --runner DirectRunner \
         --project floracast-firestore \
         --mode "eval" \
         --random_location /Users/m/Desktop/phenograph/infra/src/bitbucket.org/heindl/dataflow/gs/floracast-models/random \
         --random_occurrence_count 1 \
         --job_name floracast-fetch-random
#         --setup_file /Users/m/Desktop/phenograph/infra/src/bitbucket.org/heindl/dataflow/fetcher/setup.py
#         --requirements_file /Users/m/Desktop/phenograph/infra/src/bitbucket.org/heindl/dataflow/floracast_preprocess/requirements.txt
fi

if [ $1 == "remote" ]; then

    python ./fetcher/main.py \
        --runner=DataflowRunner \
        --mode="eval" \
        --temp_location="gs://floracast-datamining/temp" \
        --staging_location="gs://floracast-datamining/staging" \
        --random_location "gs://floracast-datamining/random" \
        --random_occurrence_count 3000 \
        --job_name floracast-fetch-random \
        --setup_file /Users/m/Desktop/phenograph/infra/src/bitbucket.org/heindl/dataflow/fetcher/setup.py
#        --requirements_file /Users/m/Desktop/phenograph/infra/src/bitbucket.org/heindl/dataflow/floracast_preprocess/requirements.txt
      # --protected_area_count 3 \
fi