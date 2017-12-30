#!/usr/bin/env bash

# gsutil cp -r gs://floracast-datamining/random /tmp/floracast-datamining/
# gsutil cp -r gs://floracast-datamining/occurrences /tmp/floracast-datamining/
# gsutil cp -r gs://floracast-datamining/protected_areas /tmp/floracast-datamining/

DATE=$(date '+%s')
TAXON=$1
TAXON_PATH="/tmp/floracast-datamining/occurrences/$TAXON"
TAXON_DATE=$(ls -l $TAXON_PATH | grep '^d' | sed 's/.* //' | sort -n | tail -1)
RANDOM_PATH="/tmp/floracast-datamining/random/"
RANDOM_DATE=$(ls -l $RANDOM_PATH | grep '^d' | sed 's/.* //' | sort -n | tail -1)

#118078 119528 130925 47392  473935 48443  48494  48529  53713  56318  58682  60782

#mkdir -p "/tmp/$OCCURRENCE_PATH"
#gsutil rsync -d -r "gs://$OCCURRENCE_PATH" "/tmp/$OCCURRENCE_PATH"
#mkdir -p "/tmp/$RANDOM_PATH"
#gsutil rsync -d -r "gs://$RANDOM_PATH" "/tmp/$RANDOM_PATH"

TRANSFORMED_PATH="/tmp/floracast-datamining/transformed/$TAXON/$DATE"

#rm -rf "$TRANSFORMED_PATH"
#mkdir -p

#echo "$TAXON_PATH/$TAXON_DATE"
#echo "$RANDOM_PATH/$RANDOM_DATE"
#echo $TRANSFORMED_PATH

python ./main.py \
    --runner=DirectRunner \
    --job_name="floracast-transform" \
    --occurrence_location="$TAXON_PATH/$TAXON_DATE" \
    --temp_location="/tmp/floracast-datamining/temp" \
    --random_location="$RANDOM_PATH/$RANDOM_DATE" \
    --output_location="$TRANSFORMED_PATH" \
    --mode "train" \
    --percent_eval 10 \
    --setup_file /Users/m/Desktop/phenograph/infra/src/bitbucket.org/heindl/dataflow/transform/setup.py \
    --workerLogLevelOverrides=com.google.cloud.dataflow#DEBUG