#!/usr/bin/env bash

python ./main.py \
         --runner DirectRunner \
         --project floracast-firestore \
         --output_location /Users/m/Desktop/phenograph/infra/src/bitbucket.org/heindl/dataflow/gs/floracast-models/random_areas \
         --job_name floracast-fetch-random \
         --random_area_count 1 \
         --extra_package dist/shared-0.0.1.tar.gz \
         --setup_file /Users/m/Desktop/phenograph/infra/src/bitbucket.org/heindl/dataflow/fetch_random_areas/setup.py