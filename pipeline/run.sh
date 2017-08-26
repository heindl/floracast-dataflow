#!/usr/bin/env bash


if [ $1 == "remote" ]; then
    python -m main \
             --runner=DataflowRunner \
             --project=floracast-20c01 \
             --output=gs://occurrence_dataflow/tfrecords/occurrences.tfrecords \
             --temp_location=gs://occurrence_dataflow/temp/ \
             --job_name=occurrence-tfrecords-1 \
             --staging_location=gs://occurrence_dataflow/staging/ \
             --setup_file ./setup.py \
             --region=us-east1 \
             --taxon=58583 \
             --disk_size_gb=100
#             --num_workers=8
#             --extra_package=./entity
#

elif [ $1 == "local" ]; then
    python -m main \
         --runner DirectRunner \
         --project floracast-20c01 \
         --temp_location /tmp/tftemp/ \
         --output ./occurrences.tfrecord \
         --staging_location /tmp/tfstaging/ \
         --job_name=occurrence_dataflow \
         --taxon=60606
fi