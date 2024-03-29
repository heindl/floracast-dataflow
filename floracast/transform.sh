#!/usr/bin/env bash

# gsutil cp -r gs://floracast-datamining/random /tmp/floracast-datamining/
# gsutil cp -r gs://floracast-datamining/occurrences /tmp/floracast-datamining/
# gsutil cp -r gs://floracast-datamining/protected_areas /tmp/floracast-datamining/

DATE=$(date '+%s')
#TAXON=$1
#TAXON_PATH="/tmp/floracast-datamining/occurrences/$TAXON"
#TAXON_DATE=$(ls -l $TAXON_PATH | grep '^d' | sed 's/.* //' | sort -n | tail -1)
#RANDOM_PATH="/tmp/floracast-datamining/random/"
#RANDOM_DATE=$(ls -l $RANDOM_PATH | grep '^d' | sed 's/.* //' | sort -n | tail -1)

#118078 119528 130925 47392  473935 48443  48494  48529  53713  56318  58682  60782

#mkdir -p "/tmp/$OCCURRENCE_PATH"
#gsutil rsync -d -r "gs://$OCCURRENCE_PATH" "/tmp/$OCCURRENCE_PATH"
#mkdir -p "/tmp/$RANDOM_PATH"
#gsutil rsync -d -r "gs://$RANDOM_PATH" "/tmp/$RANDOM_PATH"

#TRANSFORMED_PATH="/tmp/floracast-datamining/transformed/$TAXON/$DATE"
#TRANSFORMED_PATH="/tmp/floracast-datamining/transformed/aho2iyxvo37rjezikho6xbwmq/$DATE"

#rm -rf "$TRANSFORMED_PATH"
#mkdir -p

#echo "$TAXON_PATH/$TAXON_DATE"
#echo "$RANDOM_PATH/$RANDOM_DATE"
#echo $TRANSFORMED_PATH

#OCCURRENCE_LOCATION="$TAXON_PATH/$TAXON_DATE"
#RANDOM_LOCATION="$RANDOM_PATH/$RANDOM_DATE"

python ./transform.py \
    --runner=DirectRunner \
    --job_name="tensorflow-transform-$DATE" \
    --bucket="floracast-datamining" \
    --temp_location="/tmp/floracast-datamining/temp" \
    --setup_file /Users/m/Desktop/floracast/dataflow/floracast/setup.py
#    --workerLogLevelOverrides=com.google.cloud.dataflow#DEBUG