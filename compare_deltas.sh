#!/bin/bash
# Compare opt-out records between regular and SQS delta folders for a given date

set -e

BUCKET="${OPTOUT_S3_BUCKET:-}"
REGULAR_PREFIX="${REGULAR_PREFIX:-optout/delta/}"
SQS_PREFIX="${SQS_PREFIX:-sqs-delta/delta/}"

show_usage() {
    echo "Usage: $0 <date> [options]"
    echo "   OR: $0 --date <date> [options]"
    echo ""
    echo "Compare opt-out records between regular and SQS delta folders for a specific date."
    echo ""
    echo "Arguments:"
    echo "  <date>                    Date folder to compare (e.g., 2025-11-07)"
    echo ""
    echo "Options:"
    echo "  --date <date>             Date folder (alternative to positional arg)"
    echo "  --bucket <name>           S3 bucket name (or set OPTOUT_S3_BUCKET env var)"
    echo "  --regular-prefix <path>   Regular delta prefix (default: optout/delta/)"
    echo "  --sqs-prefix <path>       SQS delta prefix (default: sqs-delta/delta/)"
    echo "  --show-samples <n>        Number of sample differences to show (default: 10)"
    echo ""
    echo "Examples:"
    echo "  # Positional date with env variable"
    echo "  export OPTOUT_S3_BUCKET=my-bucket"
    echo "  $0 2025-11-07"
    echo ""
    echo "  # Using --date flag"
    echo "  $0 --date 2025-11-07 --bucket my-bucket"
    echo ""
    echo "  # Mixed style"
    echo "  $0 --bucket my-bucket 2025-11-07"
    echo ""
    echo "  # Custom prefixes"
    echo "  $0 --date 2025-11-07 --bucket my-bucket --regular-prefix optout-v2/delta --sqs-prefix sqs-delta/delta"
}

# Parse arguments
DATE=""
EXTRA_ARGS=()

while [[ $# -gt 0 ]]; do
    case $1 in
        -h|--help)
            show_usage
            exit 0
            ;;
        --bucket)
            BUCKET="$2"
            shift 2
            ;;
        --date)
            DATE="$2"
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
        -*)
            echo "Error: Unknown option: $1"
            show_usage
            exit 1
            ;;
        *)
            if [ -z "$DATE" ]; then
                DATE="$1"
            else
                echo "Error: Unknown argument: $1"
                show_usage
                exit 1
            fi
            shift
            ;;
    esac
done

# Strip trailing slash from date if present
DATE="${DATE%/}"

if [ -z "$DATE" ]; then
    echo "Error: Date argument is required"
    echo ""
    show_usage
    exit 1
fi

if [ -z "$BUCKET" ]; then
    echo "Error: S3 bucket not specified"
    echo "Set OPTOUT_S3_BUCKET environment variable or use --bucket option"
    echo ""
    show_usage
    exit 1
fi

# Check if Python script exists
if [ ! -f "compare_delta_folders.py" ]; then
    echo "Error: compare_delta_folders.py not found in current directory"
    exit 1
fi

# Run the comparison
python3 compare_delta_folders.py \
    --bucket "$BUCKET" \
    --date "$DATE" \
    --regular-prefix "$REGULAR_PREFIX" \
    --sqs-prefix "$SQS_PREFIX" \
    "${EXTRA_ARGS[@]}"

