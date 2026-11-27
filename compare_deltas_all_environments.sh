#!/bin/bash

AVAILABLE_ENVS="uid2-test, euid-integ, uid2-integ, euid-prod, uid2-prod"

show_help() {
    echo "Usage: $0 [--env ENV] [DATE...]"
    echo ""
    echo "Compare opt-out deltas between regular and SQS pipelines across environments."
    echo ""
    echo "Options:"
    echo "  --env, -e ENV    Run only for specified environment"
    echo "  --help, -h       Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0                              # All envs, yesterday+today"
    echo "  $0 2025-12-15                   # All envs, specific date"
    echo "  $0 --env uid2-prod 2025-12-15   # Single env, specific date"
    echo ""
    echo "Available environments: $AVAILABLE_ENVS"
}

ENV_FILTER=""
DATES=""

while [[ $# -gt 0 ]]; do
    case $1 in
        --help|-h)
            show_help
            exit 0
            ;;
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

if [ -z "$DATES" ]; then
    DATES="--date $(date -v-1d +%Y-%m-%d) --date $(date +%Y-%m-%d)"
fi

ENVS_RUN=0

run_comparison() {
    local env_name="$1"
    local account="$2"
    local role="$3"
    local bucket="$4"
    local regular_prefix="$5"
    local sqs_prefix="$6"
    
    local env_lower=$(echo "$env_name" | tr '[:upper:]' '[:lower:]')
    if [ -n "$ENV_FILTER" ] && [ "$env_lower" != "$ENV_FILTER" ]; then
        return
    fi
    
    ENVS_RUN=$((ENVS_RUN + 1))
    echo ""
    echo "======================================== $env_name ========================================"
    echo ""
    
    if ! aws-sso exec --account "$account" --role "$role" -- \
        bash -c "cd /Users/ian.nara/service/uid2-optout && source .venv/bin/activate && export OPTOUT_S3_BUCKET=$bucket && ./compare_deltas.sh $DATES --regular-prefix $regular_prefix --sqs-prefix $sqs_prefix" 2>&1; then
        echo "❌ Command failed for $env_name"
    fi
    
    echo ""
    echo "======================================== END $env_name ========================================"
    echo ""
}

run_comparison "UID2-TEST" "072245134533" "scrum-uid2-full-access" "uid2-optout-test-store" "optout-legacy/delta/" "optout/delta/"
run_comparison "EUID-INTEG" "101244608629" "scrum-uid2-elevated" "euid-optout-integ-store" "optout-legacy/delta/" "optout/delta/"
run_comparison "UID2-INTEG" "150073873184" "scrum-uid2-elevated" "uid2-optout-integ-store" "optout-legacy/delta/" "uid2-optout-integ/delta/"
run_comparison "EUID-PROD" "409985233527" "scrum-uid2-elevated" "euid-optout-prod-store" "optout-legacy/delta/" "optout/delta/"
run_comparison "UID2-PROD" "553165044900" "scrum-uid2-elevated" "uid2-optout-prod-store" "optout-legacy/delta/" "optout-v2/delta/"

if [ $ENVS_RUN -eq 0 ]; then
    echo "❌ No matching environment found for: $ENV_FILTER"
    echo "Available: $AVAILABLE_ENVS"
    exit 1
fi
