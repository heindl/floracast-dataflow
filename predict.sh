#!/usr/bin/env bash

python ./trainer/predict.py \
 --train_data_path "/Users/m/Desktop/phenograph/infra/src/bitbucket.org/heindl/dataflow/gs/floracast-models/transformed/1506108198" \
 --model_dir "/Users/m/Desktop/phenograph/infra/src/bitbucket.org/heindl/dataflow/gs/floracast-models/models/1506127078" \
 --output_path "/Users/m/Desktop/phenograph/infra/src/bitbucket.org/heindl/dataflow/gs/floracast-models/predictions/1506910205" \
 --raw_data_path "/Users/m/Desktop/phenograph/infra/src/bitbucket.org/heindl/dataflow/gs/floracast-models/forests/1508435803" \
 --hidden_units 60 120 60