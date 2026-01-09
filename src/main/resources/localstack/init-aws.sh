#!/usr/bin/env bash

echo "Starting optout LocalStack initialization..."

date="$(date '+%Y-%m-%d')"
full_ts="$(date '+%Y-%m-%dT%H.%M.%SZ')"
delta_file="optout-delta-000_${full_ts}_64692b14.dat"

echo "Creating S3 bucket..."
aws s3 --endpoint-url http://localhost:5001 mb s3://test-optout-bucket || echo "Bucket may already exist"

echo "Copying delta files to S3..."
aws s3 --endpoint-url http://localhost:5001 cp /s3/optout/optout-v2/delta/2023-01-01/ s3://test-optout-bucket/optout-v2/delta/2023-01-01/ --recursive
aws s3 --endpoint-url http://localhost:5001 cp /s3/optout/optout-v2/delta/optout-delta-000.dat "s3://test-optout-bucket/optout-v2/delta/${date}/${delta_file}"

echo "Creating SQS queue..."
aws sqs --endpoint-url http://localhost:5001 create-queue --queue-name optout-queue

echo "Verifying SQS queue..."
aws sqs --endpoint-url http://localhost:5001 get-queue-url --queue-name optout-queue

echo "Optout LocalStack initialization complete."