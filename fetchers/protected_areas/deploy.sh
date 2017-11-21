#!/usr/bin/env bash

python -m main \
         --runner DataflowRunner \
         --project floracast-firestore \
         --data_location "gs://floracast-datamining/protected_areas/" \
         --temp_location="gs://floracast-datamining/temp/" \
         --staging_location="gs://floracast-datamining/staging/" \
         --template_location="gs://floracast-datamining/templates/" \
         --extra_package dist/shared-0.0.1.tar.gz \
         --date 20171120 \
         --setup_file /Users/m/Desktop/phenograph/infra/src/bitbucket.org/heindl/dataflow/fetchers/protected_areas/setup.py