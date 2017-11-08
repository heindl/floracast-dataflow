#!/usr/bin/env bash

python ./trainer/predict.py \
 --train_data_path "/Users/m/Desktop/phenograph/infra/src/bitbucket.org/heindl/dataflow/gs/floracast-models/transformed/1509422063" \
 --model_dir "/Users/m/Desktop/phenograph/infra/src/bitbucket.org/heindl/dataflow/gs/floracast-models/models/1509424287" \
 --output_path "/Users/m/Desktop/phenograph/infra/src/bitbucket.org/heindl/dataflow/gs/floracast-models/predictions/1509424287" \
 --raw_data_path "/Users/m/Desktop/phenograph/infra/src/bitbucket.org/heindl/dataflow/gs/floracast-models/forests/1508435803" \
 --hidden_units 80 140 80