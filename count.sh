#!/usr/bin/env bash

PREFIX="gs://"

if [[ $1 == ${PREFIX}* ]] ; then

    LOCAL=${1/$PREFIX/'/tmp/'}

    mkdir -p $LOCAL

    gsutil rsync -d -r $1 $LOCAL

    python ./analytics/count_records.py --path $LOCAL
fi

if [[ $1 != ${PREFIX}* ]] ; then
    python ./analytics/count_records.py --path $1
fi