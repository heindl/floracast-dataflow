#!/usr/bin/env bash

TAXON=$1
DATE=$(date '+%s')
TRANSFORMED_PATH="/tmp/floracast-datamining/transformed/$TAXON"
TRANSFORMED_DATE=$(ls -l $TRANSFORMED_PATH | grep '^d' | sed 's/.* //' | sort -n | tail -1)
MODEL_PATH="/tmp/floracast-datamining/models/$TAXON/$DATE"

python -m task --train_data_path "$TRANSFORMED_PATH/$TRANSFORMED_DATE" --model_dir $MODEL_PATH
