#!/bin/bash
# Compare deltas across all environments
# Requires aws-sso to be installed and configured

set -e

# Initialize aggregate counters
TOTAL_REGULAR_FILES=0
TOTAL_REGULAR_ENTRIES=0
TOTAL_SQS_FILES=0
TOTAL_SQS_ENTRIES=0

# Get date arguments (default to yesterday and today if not provided)
if [ $# -eq 0 ]; then
    DATES="--date $(date -v-1d +%Y-%m-%d) --date $(date +%Y-%m-%d)"
else
    DATES=""
    for date in "$@"; do
        DATES="$DATES --date $date"
    done
fi

# Function to extract and sum statistics
extract_stats() {
    local output="$1"
    local env_name="$2"
    
    # Extract file counts
    local regular_files=$(echo "$output" | grep "Regular Delta Files:" | awk '{print $4}')
    local sqs_files=$(echo "$output" | grep "SQS Delta Files:" | awk '{print $4}')
    
    # Extract entry counts (from "Total entries:" line)
    local regular_entries=$(echo "$output" | grep -A3 "Regular Delta Files:" | grep "Total entries:" | awk '{print $3}')
    local sqs_entries=$(echo "$output" | grep -A3 "SQS Delta Files:" | grep "Total entries:" | awk '{print $3}')
    
    if [ -n "$regular_files" ] && [ -n "$sqs_files" ]; then
        echo "  $env_name: Regular $regular_files files/$regular_entries entries, SQS $sqs_files files/$sqs_entries entries"
        # Remove commas from numbers before arithmetic
        regular_files_clean=${regular_files//,/}
        regular_entries_clean=${regular_entries//,/}
        sqs_files_clean=${sqs_files//,/}
        sqs_entries_clean=${sqs_entries//,/}
        TOTAL_REGULAR_FILES=$((TOTAL_REGULAR_FILES + regular_files_clean))
        TOTAL_REGULAR_ENTRIES=$((TOTAL_REGULAR_ENTRIES + regular_entries_clean))
        TOTAL_SQS_FILES=$((TOTAL_SQS_FILES + sqs_files_clean))
        TOTAL_SQS_ENTRIES=$((TOTAL_SQS_ENTRIES + sqs_entries_clean))
    fi
}

echo "================================"
echo "Comparing Deltas - UID2 TEST"
echo "================================"
OUTPUT=$(aws-sso exec --account 072245134533 --role scrum-uid2-full-access -- \
    bash -c "cd /Users/ian.nara/service/uid2-optout && source .venv/bin/activate && export OPTOUT_S3_BUCKET=uid2-optout-test-store && ./compare_deltas.sh $DATES --regular-prefix optout/delta/ --sqs-prefix sqs-delta/delta/ --quiet" 2>&1)
echo "$OUTPUT"
extract_stats "$OUTPUT" "UID2-TEST"

echo "================================"
echo "Comparing Deltas - EUID INTEG"
echo "================================"
OUTPUT=$(aws-sso exec --account 101244608629 --role scrum-uid2-elevated -- \
    bash -c "cd /Users/ian.nara/service/uid2-optout && source .venv/bin/activate && export OPTOUT_S3_BUCKET=euid-optout-integ-store && ./compare_deltas.sh $DATES --regular-prefix optout/delta/ --sqs-prefix sqs-delta/delta/ --quiet" 2>&1)
echo "$OUTPUT"
extract_stats "$OUTPUT" "EUID-INTEG"

echo "================================"
echo "Comparing Deltas - UID2 INTEG"
echo "================================"
OUTPUT=$(aws-sso exec --account 150073873184 --role scrum-uid2-elevated -- \
    bash -c "cd /Users/ian.nara/service/uid2-optout && source .venv/bin/activate && export OPTOUT_S3_BUCKET=uid2-optout-integ-store && ./compare_deltas.sh $DATES --regular-prefix uid2-optout-integ/delta/ --sqs-prefix sqs-delta/delta/ --quiet" 2>&1)
echo "$OUTPUT"
extract_stats "$OUTPUT" "UID2-INTEG"

echo "================================"
echo "Comparing Deltas - EUID PROD"
echo "================================"
OUTPUT=$(aws-sso exec --account 248068286741 --role scrum-uid2-elevated -- \
    bash -c "cd /Users/ian.nara/service/uid2-optout && source .venv/bin/activate && export OPTOUT_S3_BUCKET=euid-optout && ./compare_deltas.sh $DATES --regular-prefix optout/delta/ --sqs-prefix sqs-delta/delta/ --quiet" 2>&1)
echo "$OUTPUT"
extract_stats "$OUTPUT" "EUID-PROD"

echo "================================"
echo "Comparing Deltas - UID2 PROD"
echo "================================"
OUTPUT=$(aws-sso exec --account 475720075663 --role scrum-uid2-elevated -- \
    bash -c "cd /Users/ian.nara/service/uid2-optout && source .venv/bin/activate && export OPTOUT_S3_BUCKET=uid2-optout && ./compare_deltas.sh $DATES --regular-prefix optout-v2/delta/ --sqs-prefix sqs-delta/delta/ --quiet" 2>&1)
echo "$OUTPUT"
extract_stats "$OUTPUT" "UID2-PROD"

echo ""
echo "================================================================================"
echo "📊 AGGREGATE EFFICIENCY SUMMARY"
echo "================================================================================"
echo ""
echo "Environment Breakdown:"

echo ""
echo "Total Across All Environments:"
echo "  Regular Delta:  $TOTAL_REGULAR_FILES files, $TOTAL_REGULAR_ENTRIES entries"
echo "  SQS Delta:      $TOTAL_SQS_FILES files, $TOTAL_SQS_ENTRIES entries"
echo ""

# Calculate efficiency multipliers
if [ $TOTAL_SQS_FILES -gt 0 ] && [ $TOTAL_SQS_ENTRIES -gt 0 ]; then
    FILE_EFFICIENCY=$(awk "BEGIN {printf \"%.2f\", $TOTAL_REGULAR_FILES / $TOTAL_SQS_FILES}")
    ENTRY_EFFICIENCY=$(awk "BEGIN {printf \"%.2f\", $TOTAL_REGULAR_ENTRIES / $TOTAL_SQS_ENTRIES}")
    FILE_REDUCTION=$(awk "BEGIN {printf \"%.1f\", (($TOTAL_REGULAR_FILES - $TOTAL_SQS_FILES) * 100.0) / $TOTAL_REGULAR_FILES}")
    ENTRY_REDUCTION=$(awk "BEGIN {printf \"%.1f\", (($TOTAL_REGULAR_ENTRIES - $TOTAL_SQS_ENTRIES) * 100.0) / $TOTAL_REGULAR_ENTRIES}")
    
    echo "SQS Efficiency Gains:"
    echo "  📁 Files:   ${FILE_EFFICIENCY}x fewer files (${FILE_REDUCTION}% reduction)"
    echo "  📝 Entries: ${ENTRY_EFFICIENCY}x fewer entries (${ENTRY_REDUCTION}% reduction)"
else
    echo "⚠️  Unable to calculate efficiency (no SQS data)"
fi

echo ""
echo "================================"
echo "All environments compared!"
echo "================================"
