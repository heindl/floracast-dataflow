#!/usr/bin/env bash

# Maybe just use this for now, which uses app engine to run chron without calling template

DATE=$(date '+%s')

python -m main \
         --runner DataflowRunner \
         --project floracast-firestore \
         --temp_location="gs://floracast-datamining/temp" \
         --staging_location="gs://floracast-datamining/staging" \
         --template_location="gs://floracast-datamining/templates/fetch_protected_areas/$DATE" \
         --extra_package dist/fetch_shared-0.0.1.tar.gz \
         --setup_file /Users/m/Desktop/phenograph/infra/src/bitbucket.org/heindl/dataflow/fetch_protected_areas/setup.py
#         --date 20171120 \
#         --data_location "gs://floracast-datamining/protected_areas"