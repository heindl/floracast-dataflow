#!/usr/bin/env bash

currentDateTs=$(date -j -f "%Y-%m-%d" "2016-11-09" "+%s")
endDateTs=$(date -j -f "%Y-%m-%d" "2016-11-09" "+%s")
# Seconds in a week
offset=604800

while [ "$currentDateTs" -le "$endDateTs" ]
do
  date=$(date -j -f "%s" $currentDateTs "+%Y%m%d")
  python ./call.py --bucket=floracast-datamining --date=${date}
  echo $date
  currentDateTs=$(($currentDateTs+$offset))
done