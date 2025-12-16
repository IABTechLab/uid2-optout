#!/bin/bash
# Compare deltas across all environments
# Requires aws-sso to be installed and configured
#
# Usage:
#   ./compare_deltas_all_environments.sh [--env ENV] [DATE...]
#
# Examples:
#   ./compare_deltas_all_environments.sh                           # All envs, yesterday+today
#   ./compare_deltas_all_environments.sh 2025-12-15                # All envs, specific date
#   ./compare_deltas_all_environments.sh --env uid2-test           # Single env, yesterday+today
#   ./compare_deltas_all_environments.sh --env uid2-prod 2025-12-15 # Single env, specific date
#
# Available environments: uid2-test, euid-integ, uid2-integ, euid-prod, uid2-prod

set -e

# Parse arguments
ENV_FILTER=""
DATES=""

while [[ $# -gt 0 ]]; do
    case $1 in
        --env|-e)
            ENV_FILTER=$(echo "$2" | tr '[:upper:]' '[:lower:]')
            shift 2
            ;;
        *)
            DATES="$DATES --date $1"
            shift
            ;;
    esac
done

# Default to yesterday and today if no dates provided
if [ -z "$DATES" ]; then
    DATES="--date $(date -v-1d +%Y-%m-%d) --date $(date +%Y-%m-%d)"
fi

# Initialize aggregate counters
TOTAL_REGULAR_FILES=0
TOTAL_REGULAR_ENTRIES=0
TOTAL_SQS_FILES=0
TOTAL_SQS_ENTRIES=0
ENVS_RUN=0

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

run_comparison() {
    local env_name="$1"
    local account="$2"
    local role="$3"
    local bucket="$4"
    local regular_prefix="$5"
    local sqs_prefix="$6"
    
    # Check if we should skip this environment
    local env_lower=$(echo "$env_name" | tr '[:upper:]' '[:lower:]')
    if [ -n "$ENV_FILTER" ] && [ "$env_lower" != "$ENV_FILTER" ]; then
        return
    fi
    
    ENVS_RUN=$((ENVS_RUN + 1))
    echo "======================================== $env_name ========================================"
    OUTPUT=$(aws-sso exec --account "$account" --role "$role" -- \
        bash -c "cd /Users/ian.nara/service/uid2-optout && source .venv/bin/activate && export OPTOUT_S3_BUCKET=$bucket && ./compare_deltas.sh $DATES --regular-prefix $regular_prefix --sqs-prefix $sqs_prefix --quiet" 2>&1)
    # Filter out the separator lines from compare_deltas.sh output
    echo "$OUTPUT" | grep -v "^====" | grep -v "^$"
    extract_stats "$OUTPUT" "$env_name"
    echo ""
}

run_comparison "UID2-TEST" "072245134533" "scrum-uid2-full-access" "uid2-optout-test-store" "optout/delta/" "sqs-delta/delta/"
run_comparison "EUID-INTEG" "101244608629" "scrum-uid2-elevated" "euid-optout-integ-store" "optout/delta/" "sqs-delta/delta/"
run_comparison "UID2-INTEG" "150073873184" "scrum-uid2-elevated" "uid2-optout-integ-store" "uid2-optout-integ/delta/" "sqs-delta/delta/"
run_comparison "EUID-PROD" "409985233527" "scrum-uid2-elevated" "euid-optout-prod-store" "optout/delta/" "sqs-delta/delta/"
run_comparison "UID2-PROD" "553165044900" "scrum-uid2-elevated" "uid2-optout-prod-store" "optout-v2/delta/" "sqs-delta/delta/"

# Only show summary if we ran environments
if [ $ENVS_RUN -eq 0 ]; then
    echo "❌ No matching environment found for: $ENV_FILTER"
    echo "Available: uid2-test, euid-integ, uid2-integ, euid-prod, uid2-prod"
    exit 1
fi

# Only show summary for multiple environments
if [ $ENVS_RUN -gt 1 ]; then
    echo "======================================== SUMMARY ========================================"
    echo "Total: Regular $TOTAL_REGULAR_FILES files/$TOTAL_REGULAR_ENTRIES entries, SQS $TOTAL_SQS_FILES files/$TOTAL_SQS_ENTRIES entries"

    if [ $TOTAL_SQS_FILES -gt 0 ] && [ $TOTAL_SQS_ENTRIES -gt 0 ]; then
        FILE_EFFICIENCY=$(awk "BEGIN {printf \"%.1f\", $TOTAL_REGULAR_FILES / $TOTAL_SQS_FILES}")
        ENTRY_EFFICIENCY=$(awk "BEGIN {printf \"%.1f\", $TOTAL_REGULAR_ENTRIES / $TOTAL_SQS_ENTRIES}")
        echo "Efficiency: ${FILE_EFFICIENCY}x fewer files, ${ENTRY_EFFICIENCY}x fewer entries"
    fi
fi
