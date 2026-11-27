#!/bin/bash
# Internal script - run via compare_deltas_all_environments.sh

set -e

die() { echo "Error: $1" >&2; exit 1; }

BUCKET="${OPTOUT_S3_BUCKET:-}"
REGULAR_PREFIX="${REGULAR_PREFIX:-optout/delta/}"
SQS_PREFIX="${SQS_PREFIX:-sqs-delta/delta/}"

DATES=()
EXTRA_ARGS=()

while [[ $# -gt 0 ]]; do
    case $1 in
        -h|--help)
            echo "Internal script - use compare_deltas_all_environments.sh instead"
            exit 0
            ;;
        --bucket)
            BUCKET="$2"
            shift 2
            ;;
        --date)
            DATES+=("$2")
            shift 2
            ;;
        --regular-prefix)
            REGULAR_PREFIX="$2"
            EXTRA_ARGS+=("--regular-prefix" "$2")
            shift 2
            ;;
        --sqs-prefix)
            SQS_PREFIX="$2"
            EXTRA_ARGS+=("--sqs-prefix" "$2")
            shift 2
            ;;
        --show-samples)
            EXTRA_ARGS+=("--show-samples" "$2")
            shift 2
            ;;
        --quiet|-q)
            EXTRA_ARGS+=("--quiet")
            shift
            ;;
        -*)
            die "Unknown option: $1"
            ;;
        *)
            DATES+=("$1")
            shift
            ;;
    esac
done

for i in "${!DATES[@]}"; do
    DATES[$i]="${DATES[$i]%/}"
done

[ ${#DATES[@]} -eq 0 ] && die "At least one date argument is required"
[ -z "$BUCKET" ] && die "OPTOUT_S3_BUCKET not set"
[ ! -f "compare_delta_folders.py" ] && die "compare_delta_folders.py not found"

DATE_ARGS=()
for date in "${DATES[@]}"; do
    DATE_ARGS+=("--date" "$date")
done

python3 compare_delta_folders.py \
    --bucket "$BUCKET" \
    "${DATE_ARGS[@]}" \
    --regular-prefix "$REGULAR_PREFIX" \
    --sqs-prefix "$SQS_PREFIX" \
    "${EXTRA_ARGS[@]}"
