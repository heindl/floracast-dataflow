#!/usr/bin/env bash

DATE=$(date '+%s')

PREFIX="gs://"

TRANSFORMED_PATH=$1
OUTPUT_PATH=$2
LOCAL_OUTPUT_PATH=$2

if [[ $1 == ${PREFIX}* ]]; then
    TRANSFORMED_PATH=${1/$PREFIX/'/tmp/'}
    mkdir -p $TRANSFORMED_PATH
    gsutil rsync -d -r $1 $TRANSFORMED_PATH
fi

if [[ $2 == ${PREFIX}* ]]; then
    LOCAL_OUTPUT_PATH=${2/$PREFIX/'/tmp/'}
    mkdir -p $LOCAL_OUTPUT_PATH
fi


python -m task --train_data_path $TRANSFORMED_PATH --output_path $LOCAL_OUTPUT_PATH
#  #      --num_classes 2 \
#  #      --eval_steps 20 \
#  #      --batch_size 512
#  #      --hidden_units 80 140 80 \
#  #      --train_set_size 7361 \
#
if [ "$LOCAL_OUTPUT_PATH" != "$OUTPUT_PATH" ]; then
    gsutil cp -r  $LOCAL_OUTPUT_PATH $OUTPUT_PATH
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