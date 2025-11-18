#!/usr/bin/env python3
"""
Compare opt-out records between regular delta and SQS delta folders for given date(s).

This script downloads all delta files from both folders and verifies that all opt-out
records in the regular delta folder are present in the SQS delta folder.

Delta file format: Each entry is 72 bytes (32-byte hash + 32-byte ID + 8-byte timestamp)

Usage:
    python3 compare_delta_folders.py --bucket my-bucket --date 2025-11-07
    python3 compare_delta_folders.py --bucket my-bucket --date 2025-11-07 --date 2025-11-08
    python3 compare_delta_folders.py --bucket my-bucket --date 2025-11-07 \\
        --regular-prefix optout-v2/delta --sqs-prefix sqs-delta/delta
"""

import argparse
import struct
import sys
import traceback
from datetime import datetime
from typing import List, Set, Tuple

try:
    import boto3
    from botocore.exceptions import ClientError
except ImportError:
    print("Error: boto3 not installed. Run: pip install boto3")
    sys.exit(1)


class OptOutRecord:
    """Represents a single opt-out record (hash + id + timestamp + metadata)"""

    ENTRY_SIZE = 72  # 32 (identity_hash) + 32 (advertising_id) + 7 (timestamp) + 1 (metadata)

    def __init__(self, identity_hash: bytes, advertising_id: bytes, timestamp: int):
        self.identity_hash = identity_hash
        self.advertising_id = advertising_id
        self.timestamp = timestamp

    def is_sentinel(self) -> bool:
        """Check if this is a sentinel entry (start or end)"""
        return (self.identity_hash == b'\x00' * 32 or
                self.identity_hash == b'\xff' * 32)

    def __hash__(self):
        """Return hash for set/dict operations (only hash+id, not timestamp)"""
        return hash((self.identity_hash, self.advertising_id))

    def __eq__(self, other):
        """Compare two OptOutRecord instances for equality (only hash+id, not timestamp)"""
        if not isinstance(other, OptOutRecord):
            return False
        return (self.identity_hash == other.identity_hash and
                self.advertising_id == other.advertising_id)

    def __repr__(self):
        """Return string representation of the opt-out record"""
        hash_hex = self.identity_hash.hex()[:16]
        id_hex = self.advertising_id.hex()[:16]
        try:
            dt = datetime.fromtimestamp(self.timestamp)
            dt_str = dt.strftime('%Y-%m-%d %H:%M:%S')
        except (ValueError, OSError, OverflowError):
            dt_str = "INVALID_TS"
        return f"OptOutRecord(hash={hash_hex}..., id={id_hex}..., ts={self.timestamp} [{dt_str}])"


def parse_records_from_file(data: bytes) -> List[OptOutRecord]:
    """Parse opt-out records from a delta file, skipping sentinels and invalid records"""
    records = []
    offset = 0
    entry_size = OptOutRecord.ENTRY_SIZE  # 72 bytes: 32 + 32 + 8

    # Valid timestamp range: Jan 1, 2020 to Jan 1, 2100
    MIN_VALID_TIMESTAMP = 1577836800  # 2020-01-01
    MAX_VALID_TIMESTAMP = 4102444800  # 2100-01-01

    while offset + entry_size <= len(data):
        identity_hash = data[offset:offset + 32]         # 32 bytes
        advertising_id = data[offset + 32:offset + 64]   # 32 bytes
        # Read 8 bytes but mask to 7 bytes (56 bits) - last byte is metadata
        timestamp_raw = struct.unpack('<Q', data[offset + 64:offset + 72])[0]
        timestamp = timestamp_raw & 0xFFFFFFFFFFFFFF  # Mask to 56 bits (7 bytes)

        record = OptOutRecord(identity_hash, advertising_id, timestamp)

        # Skip sentinels
        if record.is_sentinel():
            offset += entry_size
            continue

        # Skip records with invalid timestamps (corrupted data)
        if timestamp < MIN_VALID_TIMESTAMP or timestamp > MAX_VALID_TIMESTAMP:
            print(f"\n   ⚠️  Skipping record with invalid timestamp: {timestamp}")
            offset += entry_size
            continue

        records.append(record)
        offset += entry_size

    return records


def download_from_s3(bucket: str, key: str) -> bytes:
    """Download file from S3"""
    try:
        s3 = boto3.client('s3')
        response = s3.get_object(Bucket=bucket, Key=key)
        return response['Body'].read()
    except ClientError as error:
        print(f"Error downloading s3://{bucket}/{key}: {error}")
        raise


def list_files_in_folder(bucket: str, prefix: str) -> List[str]:
    """List all .dat files in an S3 folder"""
    try:
        s3 = boto3.client('s3')
        files = []
        paginator = s3.get_paginator('list_objects_v2')

        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            if 'Contents' not in page:
                continue
            for obj in page['Contents']:
                if obj['Key'].endswith('.dat'):
                    files.append(obj['Key'])

        return sorted(files)
    except ClientError as error:
        print(f"Error listing files in s3://{bucket}/{prefix}: {error}")
        raise


def load_records_from_folder(
        bucket: str, prefix: str, date_folder: str, quiet: bool = False
) -> Tuple[Set[OptOutRecord], dict]:
    """Load all opt-out records from all files in a folder"""
    full_prefix = f"{prefix}{date_folder}/"

    print(f"\n📂 Loading files from s3://{bucket}/{full_prefix}")
    files = list_files_in_folder(bucket, full_prefix)

    if not files:
        print("   ⚠️  No .dat files found")
        return set(), {}

    print(f"   Found {len(files)} delta files")

    all_records = set()
    file_stats = {}

    for i, file_key in enumerate(files, 1):
        filename = file_key.split('/')[-1]
        if not quiet:
            print(f"   [{i}/{len(files)}] Downloading {filename}...", end='', flush=True)

        try:
            data = download_from_s3(bucket, file_key)
            records = parse_records_from_file(data)

            all_records.update(records)
            total_entries_in_file = len(data) // OptOutRecord.ENTRY_SIZE
            file_stats[filename] = {
                'size': len(data),
                'entries': len(records),
                'total_entries': total_entries_in_file,  # Includes sentinels
                'file_key': file_key
            }

            if not quiet:
                print(f" {len(records)} records")
        except (ClientError, struct.error, ValueError) as error:
            print(f" ERROR: {error}")
            continue

    return all_records, file_stats


def load_records_from_multiple_folders(
        bucket: str, prefix: str, date_folders: List[str], quiet: bool = False
) -> Tuple[Set[OptOutRecord], dict]:
    """Load and aggregate records from multiple date folders"""
    all_records = set()
    all_stats = {}

    print(f"\n📅 Loading records from {len(date_folders)} date folder(s)")

    for date_folder in date_folders:
        records, stats = load_records_from_folder(bucket, prefix, date_folder, quiet)
        all_records.update(records)
        all_stats.update(stats)

    return all_records, all_stats


def analyze_differences(regular_records: Set[OptOutRecord],
                        sqs_records: Set[OptOutRecord],
                        show_samples: int = 10) -> bool:
    """Analyze and report differences between record sets"""

    print("\n📊 Analysis Results")
    print(f"   Regular delta records: {len(regular_records):,}")
    print(f"   SQS delta records:     {len(sqs_records):,}")

    # Records in regular but not in SQS (MISSING from SQS)
    missing_in_sqs = regular_records - sqs_records

    # Records in SQS but not in regular (EXTRA in SQS)
    extra_in_sqs = sqs_records - regular_records

    # Common records
    common = regular_records & sqs_records

    print(f"   Common records:        {len(common):,}")
    print(f"   Missing from SQS:      {len(missing_in_sqs):,}")
    print(f"   Extra in SQS:          {len(extra_in_sqs):,}")

    all_good = True

    if missing_in_sqs:
        print(f"\n❌ MISSING: {len(missing_in_sqs)} records in regular delta are NOT in SQS delta")
        print(f"   Sample of missing records (first {min(show_samples, len(missing_in_sqs))}):")
        for i, record in enumerate(list(missing_in_sqs)[:show_samples], 1):
            print(f"      {i}. {record}")
        if len(missing_in_sqs) > show_samples:
            print(f"      ... and {len(missing_in_sqs) - show_samples} more")
        all_good = False
    else:
        print("\n✅ All records from regular delta are present in SQS delta")

    if extra_in_sqs:
        print(f"\n⚠️  EXTRA: {len(extra_in_sqs)} records in SQS delta are NOT in regular delta")
        print("   (This might be okay if SQS captured additional opt-outs)")
        print(f"   Sample of extra records (first {min(show_samples, len(extra_in_sqs))}):")
        for i, record in enumerate(list(extra_in_sqs)[:show_samples], 1):
            print(f"      {i}. {record}")
        if len(extra_in_sqs) > show_samples:
            print(f"      ... and {len(extra_in_sqs) - show_samples} more")

    return all_good


def print_file_stats(regular_stats: dict, sqs_stats: dict) -> None:
    """Print file statistics for both folders"""
    print("\n📈 File Statistics")

    print(f"\n   Regular Delta Files: {len(regular_stats)}")
    if regular_stats:
        total_size = sum(s['size'] for s in regular_stats.values())
        total_entries = sum(s['entries'] for s in regular_stats.values())
        print(f"      Total size: {total_size:,} bytes")
        print(f"      Total entries: {total_entries:,}")
        print(f"      Avg entries/file: {total_entries / len(regular_stats):.1f}")

    print(f"\n   SQS Delta Files: {len(sqs_stats)}")
    if sqs_stats:
        total_size = sum(s['size'] for s in sqs_stats.values())
        total_entries = sum(s['entries'] for s in sqs_stats.values())
        print(f"      Total size: {total_size:,} bytes")
        print(f"      Total entries: {total_entries:,}")
        print(f"      Avg entries/file: {total_entries / len(sqs_stats):.1f}")


def main() -> None:
    """Main entry point for comparing opt-out delta folders."""
    parser = argparse.ArgumentParser(
        description='Compare opt-out records between regular and SQS delta folders',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Compare folders for a specific date
  python3 compare_delta_folders.py --bucket my-bucket --date 2025-11-07
  
  # Compare across multiple dates (handles rollover)
  python3 compare_delta_folders.py --bucket my-bucket --date 2025-11-07 --date 2025-11-08
  
  # Use custom prefixes
  python3 compare_delta_folders.py --bucket my-bucket --date 2025-11-07 \\
      --regular-prefix optout-v2/delta --sqs-prefix sqs-delta/delta
        """
    )

    parser.add_argument('--bucket', required=True,
                        help='S3 bucket name')
    parser.add_argument('--date', required=True, action='append', dest='dates',
                        help='Date folder to compare (e.g., 2025-11-07). Can be specified multiple times.')
    parser.add_argument('--regular-prefix', default='optout/delta/',
                        help='S3 prefix for regular delta files (default: optout/delta/)')
    parser.add_argument('--sqs-prefix', default='sqs-delta/delta/',
                        help='S3 prefix for SQS delta files (default: sqs-delta/delta/)')
    parser.add_argument('--show-samples', type=int, default=10,
                        help='Number of sample records to show for differences (default: 10)')
    parser.add_argument('--quiet', '-q', action='store_true',
                        help='Suppress download progress output')

    args = parser.parse_args()

    # Display dates being compared
    date_display = ', '.join(args.dates) if len(args.dates) > 1 else args.dates[0]

    print("=" * 80)
    print(f"🔍 Comparing Opt-Out Delta Files for {date_display}")
    print("=" * 80)
    print(f"Bucket: {args.bucket}")
    print(f"Regular prefix: {args.regular_prefix}")
    print(f"SQS prefix: {args.sqs_prefix}")
    print(f"Date folders: {len(args.dates)}")
    for date_folder in args.dates:
        print(f"  - {date_folder}")

    try:
        # Load all records from both folders (aggregating across multiple dates)
        regular_records, regular_stats = load_records_from_multiple_folders(
            args.bucket, args.regular_prefix, args.dates, args.quiet
        )

        sqs_records, sqs_stats = load_records_from_multiple_folders(
            args.bucket, args.sqs_prefix, args.dates, args.quiet
        )

        if not regular_records and not sqs_records:
            print("\n⚠️  No records found in either folder (environment may be empty)")
            print_file_stats(regular_stats, sqs_stats)
            print("\n" + "=" * 80)
            print("✅ SUCCESS: No data to compare (empty environment)")
            print("=" * 80)
            sys.exit(0)  # Empty environment is NOT an error!

        if not regular_records:
            print("\n⚠️  No records in regular delta folder")

        if not sqs_records:
            print("\n⚠️  No records in SQS delta folder")

        # Print file statistics
        print_file_stats(regular_stats, sqs_stats)

        # Analyze differences
        all_good = analyze_differences(regular_records, sqs_records, args.show_samples)

        print("\n" + "=" * 80)
        if all_good:
            print("✅ SUCCESS: All regular delta records are present in SQS delta")
            print("=" * 80)
            sys.exit(0)
        else:
            print("❌ FAILURE: Some regular delta records are missing from SQS delta")
            print("=" * 80)
            sys.exit(1)

    except (ClientError, ValueError, OSError) as error:
        print(f"\n❌ Error: {error}")
        traceback.print_exc()
        sys.exit(1)
    except Exception as error:  # pylint: disable=broad-except
        print(f"\n❌ Unexpected error: {error}")
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
