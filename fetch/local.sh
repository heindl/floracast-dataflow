#!/usr/bin/env bash

python ./main.py \
         --runner DirectRunner \
         --data_location /tmp/floracast-models/ \
         --job_name floracast-fetch-examples \
         --nameusages n9DCwVo5ZxaMDpd9arIgexZBc \
         --setup_file /Users/m/Desktop/phenograph/infra/src/bitbucket.org/heindl/dataflow/fetch_examples/setup.py