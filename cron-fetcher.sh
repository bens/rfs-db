#!/bin/sh -e

RFS_PATH="$HOME/rfs/rfs-$(date --iso-8601=minutes).json"

curl -s 'https://www.rfs.nsw.gov.au/feeds/majorIncidents.json' \
  -H 'Accept: application/json, text/javascript, */*; q=0.01'  \
  --compressed                                                 \
  -o "${RFS_PATH}"

gzip "${RFS_PATH}"
