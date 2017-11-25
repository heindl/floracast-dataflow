#!/usr/bin/env bash



TRAIN="/tmp/floracast-datamining/transformed/1510781454"
MODEL="/tmp/floracast-datamining/models/58682/1510867888"


python ./trainer/predict.py \
 --project "floracast-firestore" \
 --bucket "floracast-datamining" \
 --transformed "gs://floracast-datamining/transformed/" \
 --model "/tmp/floracast-datamining/models/58682/1510867888" \
 --output "/Users/m/Desktop/phenograph/infra/src/bitbucket.org/heindl/dataflow/gs/floracast-models/predictions/1509424287" \
 --raw_data_path "/Users/m/Desktop/phenograph/infra/src/bitbucket.org/heindl/dataflow/gs/floracast-models/forests/1508435803"