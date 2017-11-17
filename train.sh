#!/usr/bin/env bash

if [ $1 == "local" ]; then

    PREFIX="gs://"
    TRAIN_PATH=$2
    REMOTE_OUTPUT_PATH=$3
    LOCAL_OUTPUT_PATH=$3

    if [[ $2 == ${PREFIX}* ]]; then
        TRAIN_PATH=${2/$PREFIX/'/tmp/'}
        mkdir -p $TRAIN_PATH
        gsutil rsync -d -r $2 $TRAIN_PATH
    fi

    if [[ $3 == ${PREFIX}* ]]; then
        LOCAL_OUTPUT_PATH=${3/$PREFIX/'/tmp/'}
        mkdir -p $LOCAL_OUTPUT_PATH
    fi

    python -m trainer.tassk \
      --train_data_path $TRAIN_PATH \
      --output_path $LOCAL_OUTPUT_PATH
      #      --num_classes 2 \
      #      --eval_steps 20 \
      #      --batch_size 512
      #      --hidden_units 80 140 80 \
      #      --train_set_size 7361 \

fi



#fi

#if [ $1 == "local" ]; then
#    python -m trainer.task \
#          --hidden_units 60 120 60 \
#          --batch_size 30 \
#          --train_set_size 2052 \
#          --eval_steps 5 \
#          --num_classes 2 \
#          --train_data_path /Users/m/Desktop/phenograph/infra/src/bitbucket.org/heindl/dataflow/gs/floracast-models/train/1506108198 \
#          --output_path /Users/m/Desktop/phenograph/infra/src/bitbucket.org/heindl/dataflow/gs/floracast-models/models
##              --batch_size 512 \
#
#fi