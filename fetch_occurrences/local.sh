#!/usr/bin/env bash

python ./main.py \
         --runner DirectRunner \
         --data_location /tmp/floracast-models/occurrences \
         --job_name floracast-fetch-occurrences-53713 \
         --taxa 53713 \
         --extra_package dist/fetch_shared-0.0.1.tar.gz \
         --setup_file /Users/m/Desktop/phenograph/infra/src/bitbucket.org/heindl/dataflow/fetchers/occurrences/setup.py