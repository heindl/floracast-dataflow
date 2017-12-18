#!/usr/bin/env bash

python ./main.py \
         --runner DirectRunner \
         --project floracast-firestore \
         --data_location /Users/m/Desktop/phenograph/infra/src/bitbucket.org/heindl/dataflow/gs/floracast-models/protected_areas \
         --job_name floracast-fetch-occurrences-20171110 \
         --protected_area_count 1 \
         --date 20171110 \
         --extra_package dist/fetch_shared-0.0.1.tar.gz \
         --setup_file /Users/m/Desktop/phenograph/infra/src/bitbucket.org/heindl/dataflow/fetch_protected_areas/setup.py