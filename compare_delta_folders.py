#!/usr/bin/env python3

import argparse
import struct
import sys
import traceback
from datetime import datetime
from pathlib import Path
from typing import List, Set, Tuple

try:
    import boto3
    from botocore.exceptions import ClientError
except ImportError:
    print("Error: boto3 not installed. Run: pip install boto3")
    sys.exit(1)

CACHE_DIR =  "./.cache/delta-cache/"


class OptOutRecord:
    # 32 (identity_hash) + 32 (advertising_id) + 7 (timestamp) + 1 (metadata)
    ENTRY_SIZE = 72

    def __init__(self, identity_hash: bytes, advertising_id: bytes, timestamp: int):
        self.identity_hash = identity_hash
        self.advertising_id = advertising_id
        self.timestamp = timestamp

    def is_sentinel(self) -> bool:
        return (self.identity_hash == b'\x00' * 32 or
                self.identity_hash == b'\xff' * 32)

    def __hash__(self):
        return hash((self.identity_hash, self.advertising_id))

    def __eq__(self, other):
        if not isinstance(other, OptOutRecord):
            return False
        return (self.identity_hash == other.identity_hash and
                self.advertising_id == other.advertising_id)

    def __repr__(self):
        hash_hex = self.identity_hash.hex()[:16]
        id_hex = self.advertising_id.hex()[:16]
        try:
            dt = datetime.fromtimestamp(self.timestamp)
            dt_str = dt.strftime('%Y-%m-%d %H:%M:%S')
        except (ValueError, OSError, OverflowError):
            dt_str = "INVALID_TS"
        return f"OptOutRecord(hash={hash_hex}..., id={id_hex}..., ts={self.timestamp} [{dt_str}])"


def parse_records_from_file(data: bytes) -> List[OptOutRecord]:
    records = []
    offset = 0
    entry_size = OptOutRecord.ENTRY_SIZE

    MIN_VALID_TIMESTAMP = 1577836800  # 2020-01-01
    MAX_VALID_TIMESTAMP = 4102444800  # 2100-01-01

    while offset + entry_size <= len(data):
        identity_hash = data[offset:offset + 32]
        advertising_id = data[offset + 32:offset + 64]
        # Last byte is metadata, mask to 56 bits for timestamp
        timestamp_raw = struct.unpack('<Q', data[offset + 64:offset + 72])[0]
        timestamp = timestamp_raw & 0xFFFFFFFFFFFFFF

        record = OptOutRecord(identity_hash, advertising_id, timestamp)

        if record.is_sentinel():
            offset += entry_size
            continue

        if timestamp < MIN_VALID_TIMESTAMP or timestamp > MAX_VALID_TIMESTAMP:
            offset += entry_size
            continue

        records.append(record)
        offset += entry_size

    return records


def get_cached_file(bucket: str, key: str) -> bytes | None:
    filename = key.split('/')[-1]
    cache_path = Path(CACHE_DIR) / bucket / filename
    if cache_path.exists():
        return cache_path.read_bytes()
    return None


def save_to_cache(bucket: str, key: str, data: bytes) -> None:
    filename = key.split('/')[-1]
    cache_path = Path(CACHE_DIR) / bucket / filename
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.write_bytes(data)


def download_from_s3(bucket: str, key: str) -> tuple[bytes, bool]:
    cached = get_cached_file(bucket, key)
    if cached is not None:
        return cached, True

    try:
        s3 = boto3.client('s3')
        response = s3.get_object(Bucket=bucket, Key=key)
        data = response['Body'].read()
        save_to_cache(bucket, key, data)
        return data, False
    except ClientError as error:
        print(f"\nError downloading s3://{bucket}/{key}: {error}")
        raise


def list_dat_files(bucket: str, prefix: str) -> List[str]:
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
    full_prefix = f"{prefix}{date_folder}/"
    files = list_dat_files(bucket, full_prefix)

    if not files:
        print(f"   {date_folder}: no files")
        return set(), {}

    all_records = set()
    file_stats = {}
    total_records = 0
    cached_count = 0

    for i, file_key in enumerate(files, 1):
        filename = file_key.split('/')[-1]
        if not quiet:
            cache_info = f" ({cached_count} cached)" if cached_count > 0 else ""
            print(f"\r   {date_folder}: [{i}/{len(files)}] {total_records} records{cache_info}", end='', flush=True)

        try:
            data, from_cache = download_from_s3(bucket, file_key)
            if from_cache:
                cached_count += 1
            records = parse_records_from_file(data)
            total_records += len(records)

            all_records.update(records)
            total_entries_in_file = len(data) // OptOutRecord.ENTRY_SIZE
            file_stats[filename] = {
                'size': len(data),
                'entries': len(records),
                'total_entries': total_entries_in_file,
                'file_key': file_key
            }
        except (ClientError, struct.error, ValueError) as error:
            print(f"\n   ERROR: {error}")
            continue

    if not quiet:
        cache_info = f" ({cached_count} cached)" if cached_count > 0 else ""
        print(f"\r   {date_folder}: {len(files)} files, {total_records} records{cache_info}" + " " * 20)

    return all_records, file_stats


def load_records_from_multiple_folders(
        bucket: str, prefix: str, date_folders: List[str], quiet: bool = False
) -> Tuple[Set[OptOutRecord], dict]:
    all_records = set()
    all_stats = {}

    for date_folder in date_folders:
        records, stats = load_records_from_folder(bucket, prefix, date_folder, quiet)
        all_records.update(records)
        all_stats.update(stats)

    return all_records, all_stats


def analyze_differences(regular_records: Set[OptOutRecord],
                        sqs_records: Set[OptOutRecord],
                        show_samples: int = 10) -> bool:
    print("\n\n📊 Analysis Results (unique records)")
    print(f"\n   Regular: {len(regular_records):,}")
    print(f"   SQS:     {len(sqs_records):,}")

    missing_in_sqs = regular_records - sqs_records
    extra_in_sqs = sqs_records - regular_records
    common = regular_records & sqs_records

    print(f"   Common:  {len(common):,}")
    print(f"   Missing: {len(missing_in_sqs):,}")
    print(f"   Extra:   {len(extra_in_sqs):,}")

    all_records_matched = True

    if missing_in_sqs:
        print(f"\n❌ MISSING: {len(missing_in_sqs)} records in regular are NOT in SQS")
        print(f"   Sample (first {min(show_samples, len(missing_in_sqs))}):")
        for i, record in enumerate(list(missing_in_sqs)[:show_samples], 1):
            print(f"      {i}. {record}")
        if len(missing_in_sqs) > show_samples:
            print(f"      ... and {len(missing_in_sqs) - show_samples} more")
        all_records_matched = False

    if extra_in_sqs:
        print(f"\n⚠️  EXTRA: {len(extra_in_sqs)} records in SQS are NOT in regular")
        print(f"   Sample (first {min(show_samples, len(extra_in_sqs))}):")
        for i, record in enumerate(list(extra_in_sqs)[:show_samples], 1):
            print(f"      {i}. {record}")
        if len(extra_in_sqs) > show_samples:
            print(f"      ... and {len(extra_in_sqs) - show_samples} more")

    return all_records_matched


def print_file_stats(regular_stats: dict, sqs_stats: dict) -> None:
    print("\n\n📈 File Statistics")

    print(f"\n   Regular Delta Files: {len(regular_stats)}")
    if regular_stats:
        total_size = sum(s['size'] for s in regular_stats.values())
        total_entries = sum(s['entries'] for s in regular_stats.values())
        print(f"      Total size: {total_size:,} bytes")
        print(f"      Total entries: {total_entries:,} (with duplicates)")
        print(f"      Avg entries/file: {total_entries / len(regular_stats):.1f}")

    print(f"\n   SQS Delta Files: {len(sqs_stats)}")
    if sqs_stats:
        total_size = sum(s['size'] for s in sqs_stats.values())
        total_entries = sum(s['entries'] for s in sqs_stats.values())
        print(f"      Total size: {total_size:,} bytes")
        print(f"      Total entries: {total_entries:,} (with duplicates)")
        print(f"      Avg entries/file: {total_entries / len(sqs_stats):.1f}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description='Compare opt-out records between regular and SQS delta folders'
    )
    parser.add_argument('--bucket', required=True, help='S3 bucket name')
    parser.add_argument('--date', required=True, action='append', dest='dates',
                        help='Date folder (can be specified multiple times)')
    parser.add_argument('--regular-prefix', default='optout/delta/',
                        help='S3 prefix for regular delta files')
    parser.add_argument('--sqs-prefix', default='sqs-delta/delta/',
                        help='S3 prefix for SQS delta files')
    parser.add_argument('--show-samples', type=int, default=10,
                        help='Number of sample records to show for differences')
    parser.add_argument('--quiet', '-q', action='store_true',
                        help='Suppress download progress output')

    args = parser.parse_args()

    date_display = ', '.join(args.dates)
    print(f"🔍 {args.bucket} | Dates: {date_display}")
    print(f"\n   Regular: {args.regular_prefix}")

    try:
        regular_records, regular_stats = load_records_from_multiple_folders(
            args.bucket, args.regular_prefix, args.dates, args.quiet
        )

        print(f"\n   SQS: {args.sqs_prefix}")

        sqs_records, sqs_stats = load_records_from_multiple_folders(
            args.bucket, args.sqs_prefix, args.dates, args.quiet
        )

        if not regular_records and not sqs_records:
            print("\n⚠️  No records found in either folder (environment may be empty)")
            print_file_stats(regular_stats, sqs_stats)
            print("\n✅ SUCCESS: No data to compare (empty environment)")
            sys.exit(0)

        if not regular_records:
            print("\n⚠️  No records in regular delta folder")

        if not sqs_records:
            print("\n⚠️  No records in SQS delta folder")

        print_file_stats(regular_stats, sqs_stats)

        all_records_matched = analyze_differences(regular_records, sqs_records, args.show_samples)

        if all_records_matched:
            print("\n✅ SUCCESS: All regular delta records are present in SQS delta")
            sys.exit(0)
        else:
            print("\n❌ FAILURE: Some regular delta records are missing from SQS delta")
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
