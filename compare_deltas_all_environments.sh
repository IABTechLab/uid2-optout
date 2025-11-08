#!/bin/bash
# Compare deltas across all environments
# Requires aws-sso to be installed and configured

set -e

# Get date arguments (default to yesterday and today if not provided)
if [ $# -eq 0 ]; then
    DATES="--date $(date -v-1d +%Y-%m-%d) --date $(date +%Y-%m-%d)"
else
    DATES=""
    for date in "$@"; do
        DATES="$DATES --date $date"
    done
fi

echo "================================"
echo "Comparing Deltas - UID2 TEST"
echo "================================"
aws-sso exec --account 072245134533 --role scrum-uid2-full-access -- \
    bash -c "cd /Users/ian.nara/service/uid2-optout && source .venv/bin/activate && export OPTOUT_S3_BUCKET=uid2-optout-test-store && ./compare_deltas.sh $DATES --regular-prefix optout/delta/ --sqs-prefix sqs-delta/delta/ --quiet"

echo "================================"
echo "Comparing Deltas - EUID INTEG"
echo "================================"
aws-sso exec --account 101244608629 --role scrum-uid2-elevated -- \
    bash -c "cd /Users/ian.nara/service/uid2-optout && source .venv/bin/activate && export OPTOUT_S3_BUCKET=euid-optout-integ-store && ./compare_deltas.sh $DATES --regular-prefix optout/delta/ --sqs-prefix sqs-delta/delta/ --quiet"

echo "================================"
echo "Comparing Deltas - UID2 INTEG"
echo "================================"
aws-sso exec --account 150073873184 --role scrum-uid2-elevated -- \
    bash -c "cd /Users/ian.nara/service/uid2-optout && source .venv/bin/activate && export OPTOUT_S3_BUCKET=uid2-optout-integ-store && ./compare_deltas.sh $DATES --regular-prefix uid2-optout-integ/delta/ --sqs-prefix sqs-delta/delta/ --quiet"

echo "================================"
echo "Comparing Deltas - EUID PROD"
echo "================================"
aws-sso exec --account 248068286741 --role scrum-uid2-elevated -- \
    bash -c "cd /Users/ian.nara/service/uid2-optout && source .venv/bin/activate && export OPTOUT_S3_BUCKET=euid-optout && ./compare_deltas.sh $DATES --regular-prefix optout/delta/ --sqs-prefix sqs-delta/delta/ --quiet"

echo "================================"
echo "Comparing Deltas - UID2 PROD"
echo "================================"
aws-sso exec --account 475720075663 --role scrum-uid2-elevated -- \
    bash -c "cd /Users/ian.nara/service/uid2-optout && source .venv/bin/activate && export OPTOUT_S3_BUCKET=uid2-optout && ./compare_deltas.sh $DATES --regular-prefix optout-v2/delta/ --sqs-prefix sqs-delta/delta/ --quiet"

echo ""
echo "================================"
echo "All environments compared!"
echo "================================"
