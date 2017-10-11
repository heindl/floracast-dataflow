#!/usr/bin/env bash

if [ $1 == "local" ]; then
    python -m trainer.task \
          --hidden_units 60 120 60 \
          --batch_size 30 \
          --train_set_size 2052 \
          --eval_steps 5 \
          --num_classes 2 \
          --train_data_path /Users/m/Desktop/phenograph/infra/src/bitbucket.org/heindl/dataflow/gs/floracast-models/train/1506108198 \
          --output_path /Users/m/Desktop/phenograph/infra/src/bitbucket.org/heindl/dataflow/gs/floracast-models/models
#              --batch_size 512 \

fi