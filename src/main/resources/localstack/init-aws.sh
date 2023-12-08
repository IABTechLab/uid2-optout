#!/usr/bin/env bash

aws s3 --endpoint-url http://localhost:5001 mb s3://test-optout-bucket
aws s3 --endpoint-url http://localhost:5001 cp /s3/optout/ s3://test-optout-bucket/ --recursive
