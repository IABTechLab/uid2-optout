#!/usr/bin/env bash

date="$(date '+%Y-%m-%d')"
delta_dir="/s3/optout/optout-v2/delta/${date}"

full_ts="$(date '+%Y-%m-%dT%H.%M.%SZ')"
delta_file="optout-delta-000_${full_ts}_64692b14.dat"

mkdir -p "$delta_dir"
mv /s3/optout/optout-v2/delta/optout-delta-000.dat "${delta_dir}/${delta_file}"
aws s3 --endpoint-url http://localhost:5001 mb s3://test-optout-bucket
aws s3 --endpoint-url http://localhost:5001 cp /s3/optout/ s3://test-optout-bucket/ --recursive
