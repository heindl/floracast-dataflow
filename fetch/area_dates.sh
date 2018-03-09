#!/usr/bin/env bash

START=${1:-$(date '+%Y%m%d')}
END=${2:-$(date '+%Y%m%d')}

currentTs=$(date -j -f "%Y%m%d" "$START" "+%s")
endTs=$(date -j -f "%Y%m%d" "$END" "+%s")
offset=86400 # Seconds in a day
res=""

while [ "$currentTs" -le "$endTs" ]
do

  weekDay=$(date -j -f "%s" "$currentTs" "+%u")
  if [ "$weekDay" == "1" ] || [ "$weekDay" == "5" ]; then
     d=$(date -j -f "%s" $currentTs "+%Y%m%d")
     res+=",$d"
  fi
  currentTs=$(( $currentTs + $offset ))
done

if [ ${#res} != 0 ]; then
    echo $res | cut -c 2-
fi