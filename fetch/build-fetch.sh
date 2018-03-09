#!/usr/bin/env bash

# Maybe just use this for now, which uses app engine to run chron without calling template

TIME=$(date '+%s')
AREA_DATES=$(./area_dates.sh "20180201")

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
         --job_name "floracast-fetch-$TIME" \
         --nameusages "9sYKdRe6OUgzTwabsjjuFiwVU" \
         --setup_file /Users/m/Desktop/floracast/dataflow/fetch/setup.py
#         --requirements_file /Users/m/Desktop/floracast/dataflow/fetch/requirements.txt
#
#
#--protected_areas "$AREA_DATES" \
#--random True \