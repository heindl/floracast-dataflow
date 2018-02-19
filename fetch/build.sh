#!/usr/bin/env bash

# Maybe just use this for now, which uses app engine to run chron without calling template

DATE=$(date '+%s')

python -m main \
         --runner DataflowRunner \
         --temp_location="gs://floracast-datamining/temp" \
         --staging_location="gs://floracast-datamining/staging" \
         --template_location="gs://floracast-datamining/templates/fetch_occurrences/$DATE" \
         --setup_file /Users/m/Desktop/phenograph/infra/src/bitbucket.org/heindl/dataflow/fetch_occurrences/setup.py