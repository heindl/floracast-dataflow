#!/usr/bin/env bash

python ./trainer/predict.py \
 --train_data_path "/tmp/floracast-datamining/transformed/1510781454" \
 --model_dir "/tmp/floracast-datamining/models/58682/1510867888" \
 --output_path "/Users/m/Desktop/phenograph/infra/src/bitbucket.org/heindl/dataflow/gs/floracast-models/predictions/1509424287" \
 --raw_data_path "/Users/m/Desktop/phenograph/infra/src/bitbucket.org/heindl/dataflow/gs/floracast-models/forests/1508435803"