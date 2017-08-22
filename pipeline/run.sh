#!/usr/bin/env bash


if [ $1 == "remote" ]; then
    python -m main \
             --runner=DataflowRunner \
             --project=floracast-20c01 \
             --output=gs://occurrence_dataflow/tfrecords/occurrences.tfrecords \
             --temp_location=gs://occurrence_dataflow/temp/ \
             --job_name=occurrence-tfrecords-1 \
             --staging_location=gs://occurrence_dataflow/staging/ \
             --setup_file ./setup.py
#             --extra_package=./entity
#             --num_workers=1 \
#             --disk_size_gb=500

elif [ $1 == "local" ]; then
    python -m main \
         --runner DirectRunner \
         --project floracast-20c01 \
         --temp_location /tmp/tftemp/ \
         --output ./occurrences.tfrecord \
         --staging_location /tmp/tfstaging/ \
         --job_name=occurrence_dataflow
fi