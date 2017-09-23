#!/usr/bin/env bash

python ./trainer/predict.py \
 --train_data_path "/Users/m/Desktop/phenograph/infra/src/bitbucket.org/heindl/dataflow/gs/floracast-models/transformed/1506108198" \
 --model_dir "/Users/m/Desktop/phenograph/infra/src/bitbucket.org/heindl/dataflow/gs/floracast-models/models/1506127078" \
 --output_path "/Users/m/Desktop/phenograph/infra/src/bitbucket.org/heindl/dataflow/gs/floracast-models/predictions" \
 --raw_data_file_pattern "/Users/m/Desktop/phenograph/infra/src/bitbucket.org/heindl/dataflow/gs/floracast-models/forests/1505918925/20170827/*" \
 --hidden_units 60 120 60