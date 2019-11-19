#!/bin/sh

rsync -i "$RFS_SOURCE/rfs-*.json.gz" .
python3 rfs.py
