#!/usr/bin/env bash



currentDateTs=$(date -j -f "%Y-%m-%d" "2016-11-09" "+%s")
endDateTs=$(date -j -f "%Y-%m-%d" "2017-03-29" "+%s")
# Seconds in a week
offset=604800

while [ "$currentDateTs" -le "$endDateTs" ]
do
  date=$(date -j -f "%s" $currentDateTs "+%Y%m%d")
  python -m predict --taxon 53713 --date ${date} --bucket "floracast-datamining"
  echo $date
  currentDateTs=$(($currentDateTs+$offset))
done