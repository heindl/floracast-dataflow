#!/usr/bin/env bash

# Maybe just use this for now, which uses app engine to run chron without calling template

DATE=$(date '+%s')

#python -m fetch \
#         --runner DataflowRunner \
#         --temp_location="gs://floracast-datamining/temp" \
#         --staging_location="gs://floracast-datamining/staging" \
#         --template_location="gs://floracast-datamining/templates/fetch_occurrences/$DATE" \
#         --setup_file /Users/m/Desktop/phenograph/infra/src/bitbucket.org/heindl/dataflow/fetch/setup.py

python -m fetch \
         --runner DataflowRunner \
         --data_location "gs://floracast-datamining/" \
         --temp_location="gs://floracast-datamining/temp" \
         --staging_location="gs://floracast-datamining/staging" \
         --job_name "floracast-fetch-$DATE" \
         --nameusages "AHo2IYxvo37RjezIkho6xBWmq" \
         --setup_file /Users/m/Desktop/floracast/dataflow/fetch/setup.py
#         --requirements_file /Users/m/Desktop/floracast/dataflow/fetch/requirements.txt