#!/usr/bin/env bash
set -e

echo "Starting optout LocalStack initialization..."

date="$(date '+%Y-%m-%d')"
full_ts="$(date '+%Y-%m-%dT%H.%M.%SZ')"
delta_file="optout-delta-000_${full_ts}_64692b14.dat"

echo "Creating S3 bucket..."
aws s3 --endpoint-url http://localhost:5001 mb s3://test-optout-bucket

echo "Copying delta files to S3..."
aws s3 --endpoint-url http://localhost:5001 cp /s3/optout/optout-v2/delta/2023-01-01/ s3://test-optout-bucket/optout-v2/delta/2023-01-01/ --recursive
aws s3 --endpoint-url http://localhost:5001 cp /s3/optout/optout-v2/delta/optout-delta-000.dat "s3://test-optout-bucket/optout-v2/delta/${date}/${delta_file}"

echo "Creating SQS queue..."
aws sqs --endpoint-url http://localhost:5001 create-queue --queue-name optout-queue

echo "Verifying SQS queue..."
QUEUE_URL=$(aws sqs --endpoint-url http://localhost:5001 get-queue-url --queue-name optout-queue --query 'QueueUrl' --output text)
echo "SQS queue URL returned by LocalStack: $QUEUE_URL"

echo "Listing all SQS queues..."
aws sqs --endpoint-url http://localhost:5001 list-queues

echo "Testing SQS send message..."
aws sqs --endpoint-url http://localhost:5001 send-message --queue-url "$QUEUE_URL" --message-body "test-init-message" || echo "WARNING: Test send message failed"

echo "Testing with path-style URL (localhost)..."
aws sqs --endpoint-url http://localhost:5001 send-message --queue-url "http://localhost:5001/000000000000/optout-queue" --message-body "test-path-style" || echo "WARNING: Path-style test (localhost) failed"

# Note: From inside other containers, they use 'localstack' hostname. The queue URL in config uses localstack:5001
# This test runs inside LocalStack container where 'localstack' might not resolve, so we test with localhost only

echo "Optout LocalStack initialization complete."