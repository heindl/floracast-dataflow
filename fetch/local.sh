#!/usr/bin/env bash

python ./fetch.py \
         --runner DirectRunner \
         --data_location /tmp/floracast-models/ \
         --job_name floracast-fetch-examples \
         --nameusages AHo2IYxvo37RjezIkho6xBWmq \
         --setup_file /Users/m/Desktop/phenograph/infra/src/bitbucket.org/heindl/dataflow/fetch/setup.py \
         --requirements_file /Users/m/Desktop/phenograph/infra/src/bitbucket.org/heindl/dataflow/fetch/requirements.txt