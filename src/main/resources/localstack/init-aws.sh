#!/usr/bin/env bash

echo "Starting optout LocalStack initialization..."

# Set region to match optout service config
export AWS_DEFAULT_REGION=us-east-1

date="$(date '+%Y-%m-%d')"
full_ts="$(date '+%Y-%m-%dT%H.%M.%SZ')"
delta_file="optout-delta-000_${full_ts}_64692b14.dat"

echo "Creating S3 bucket..."
awslocal s3 mb s3://test-optout-bucket || echo "Bucket may already exist"

echo "Copying delta files to S3..."
awslocal s3 cp /s3/optout/optout-v2/delta/2023-01-01/ s3://test-optout-bucket/optout-v2/delta/2023-01-01/ --recursive
awslocal s3 cp /s3/optout/optout-v2/delta/optout-delta-000.dat "s3://test-optout-bucket/optout-v2/delta/${date}/${delta_file}"

echo "Creating SQS queue..."
awslocal sqs create-queue --queue-name optout-queue
echo "Queue creation exit code: $?"

echo "Verifying SQS queue..."
awslocal sqs get-queue-url --queue-name optout-queue

echo "Listing all queues..."
awslocal sqs list-queues

echo "Optout LocalStack initialization complete."