#!/usr/bin/env bash

python -m main --output gs://floracast-occurrences/occurrences.tfrecords \
         --runner DataflowRunner \
         --project floracast-20c01 \
         --temp_location gs://floracast-occurrences/tmp/