#!/usr/bin/env bash

python ./main.py \
         --runner DirectRunner \
         --output_location /Users/m/Desktop/phenograph/infra/src/bitbucket.org/heindl/dataflow/gs/floracast-models/occurrences \
         --job_name floracast-fetch-occurrences \
         --taxa 58682 \
         --extra_package dist/shared-0.0.1.tar.gz \
         --setup_file /Users/m/Desktop/phenograph/infra/src/bitbucket.org/heindl/dataflow/fetchers/occurrences/setup.py