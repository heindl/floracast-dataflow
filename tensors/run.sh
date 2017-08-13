#!/bin/bash
set -e

curl -k -L -O https://archive.ics.uci.edu/ml/machine-learning-databases/adult/adult.data

curl -k -L -O https://archive.ics.uci.edu/ml/machine-learning-databases/adult/adult.test

# Convert CSV to TFRecord: adult.data -> adult_data.tfrecords
python tfrecord.py --data_file adult.data

# Convert CSV to TFRecord: adult.test -> adult_test.tfrecords
python tfrecord.py --data_file adult.test --skip_header

python wide_n_deep_tutorial.py \
    --model_type=wide_n_deep \
    --train_data=adult_data.tfrecords \
    --test_data=adult_test.tfrecords \
    --model_dir=model/