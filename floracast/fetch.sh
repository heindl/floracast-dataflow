#!/usr/bin/env bash

# Maybe just use this for now, which uses app engine to run chron without calling template

TIME=$(date '+%s')
AREA_DATES=$(./area_dates.sh "20170421")

#python -m fetch \
#         --runner DataflowRunner \
#         --temp_location="gs://floracast-datamining/temp" \
#         --staging_location="gs://floracast-datamining/staging" \
#         --template_location="gs://floracast-datamining/templates/fetch_occurrences/$DATE" \
#         --setup_file /Users/m/Desktop/phenograph/infra/src/bitbucket.org/heindl/dataflow/fetch/setup.py

python -m fetch \
         --runner DataflowRunner \
         --bucket "floracast-datamining" \
         --temp_location="gs://floracast-datamining/temp" \
         --staging_location="gs://floracast-datamining/staging" \
         --job_name "floracast-fetch-$TIME" \
         --random true \
         --protected_areas true \
         --protected_area_dates "$AREA_DATES" \
         --setup_file /Users/m/Desktop/floracast/dataflow/floracast/setup.py

#python -m fetch \
#         --runner DirectRunner \
#         --bucket "floracast-datamining" \
#         --temp_location="/tmp/floracast-datamining/temp" \
#         --job_name "floracast-fetch-$TIME" \
#         --protected_areas true \
#         --protected_area_dates "$AREA_DATES" \
#         --setup_file /Users/m/Desktop/floracast/dataflow/floracast/setup.py

#         --requirements_file /Users/m/Desktop/floracast/dataflow/fetch/requirements.txt
#         --name_usages "2xUhop2,8vMzLmz,BL6T9EP,MGkiZI9,MXRYVzj,PfxnNDJ,cFHSwIL,qJJlT2R,qWlT2bh,ugkG3de" \
