#!/bin/bash
set -e

python ./main.py \
 --train_data=/Users/m/Desktop/phenograph/infra/src/bitbucket.org/heindl/dataflow/tensors/records-00000-of-00001.tfrecord \
 --test_data=/Users/m/Desktop/phenograph/infra/src/bitbucket.org/heindl/dataflow/tensors/records-00000-of-00001.tfrecord