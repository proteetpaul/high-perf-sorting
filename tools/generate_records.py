#!/usr/bin/env python3
"""
Generate fixed-size key/value records and write them to a binary file.

Inputs:
- key-bytes: number of bytes in each key
- value-bytes: number of bytes in each value
- total-size: total output size, accepts suffixes like MB/GB (e.g., 1024MB, 1GB)
- out-dir: output directory path. Output file name is derived as
  <base-name>-<size>.bin (e.g., records-1GB.bin)
- base-name: optional base name for the file (default: records)
- distribution: record distribution; currently only 'uniform' is implemented

Record format per SortBenchmark convention: [key bytes][value bytes], fixed lengths.

Example:
  python tools/generate_records.py \
    --key-bytes 10 --value-bytes 90 --total-size 1GB \
    --out-dir /tmp --base-name records --distribution uniform
"""

import argparse
import math
import os
import sys
from typing import Tuple


def parse_total_size(size_str: str) -> int:
    """Parse a human-friendly size string into bytes.

    Supports case-insensitive suffixes: B, KB, MB, GB (base 1024). If no suffix is
    provided, bytes are assumed.
    """
    s = size_str.strip().lower()
    # Extract numeric part and unit suffix
    number_str = ""
    unit_str = ""
    for ch in s:
        if ch.isdigit() or ch == ".":
            number_str += ch
        else:
            unit_str += ch
    if not number_str:
        raise ValueError(f"Invalid size: '{size_str}'")
    try:
        number = float(number_str)
    except ValueError as exc:
        raise ValueError(f"Invalid numeric size: '{size_str}'") from exc

    unit_str = unit_str.strip()
    if unit_str in ("", "b"):
        multiplier = 1
    elif unit_str in ("k", "kb"):
        multiplier = 1024
    elif unit_str in ("m", "mb"):
        multiplier = 1024 ** 2
    elif unit_str in ("g", "gb"):
        multiplier = 1024 ** 3
    else:
        raise ValueError(
            f"Unsupported size unit in '{size_str}'. Use B, KB, MB, or GB."
        )
    total_bytes = int(number * multiplier)
    if total_bytes <= 0:
        raise ValueError("Total size must be > 0 bytes")
    return total_bytes


def compute_counts(total_bytes: int, key_bytes: int, value_bytes: int) -> Tuple[int, int]:
    """Compute number of records and leftover bytes that won't fit a full record."""
    record_size = key_bytes + value_bytes
    if record_size <= 0:
        raise ValueError("Record size must be > 0")
    num_records = total_bytes // record_size
    leftover = total_bytes - (num_records * record_size)
    return int(num_records), int(leftover)


def generate_uniform_bytes(num_bytes: int) -> bytes:
    """Generate cryptographically-strong random bytes (uniform over 0..255)."""
    return os.urandom(num_bytes)


def write_uniform_records(
    file_path: str,
    key_bytes: int,
    value_bytes: int,
    total_bytes: int,
) -> Tuple[int, int, int]:
    """Write uniformly random fixed-size records to file.

    Returns a tuple of (num_records_written, bytes_written, leftover_bytes).
    """
    record_size = key_bytes + value_bytes
    num_records, leftover = compute_counts(total_bytes, key_bytes, value_bytes)

    # Target batch size ~8 MiB to balance memory and syscall overhead.
    target_batch_bytes = 8 * 1024 * 1024
    records_per_batch = max(1, target_batch_bytes // record_size)

    bytes_written = 0
    written_records = 0
    with open(file_path, "wb") as f:
        remaining = num_records
        while remaining > 0:
            batch_count = min(remaining, records_per_batch)
            # Pre-allocate a bytearray for the batch to reduce reallocations.
            batch_buffer = bytearray(batch_count * record_size)
            offset = 0
            for _ in range(batch_count):
                # Key
                key = generate_uniform_bytes(key_bytes)
                batch_buffer[offset : offset + key_bytes] = key
                offset += key_bytes
                # Value
                value = generate_uniform_bytes(value_bytes)
                batch_buffer[offset : offset + value_bytes] = value
                offset += value_bytes
            f.write(batch_buffer)
            bytes_written += len(batch_buffer)
            written_records += batch_count
            remaining -= batch_count

    return written_records, bytes_written, leftover


def human_size_suffix(total_bytes: int) -> str:
    """Format a size suffix (e.g., 1GB, 512MB, 123B) using binary units.

    Prefers GB when total_bytes is a multiple of 2^30, then MB (2^20), else B.
    """
    gb = 1024 ** 3
    mb = 1024 ** 2
    if total_bytes % gb == 0:
        return f"{total_bytes // gb}GB"
    if total_bytes % mb == 0:
        return f"{total_bytes // mb}MB"
    return f"{total_bytes}B"


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Generate fixed-size key/value records to a binary file (SortBenchmark-style)."
        )
    )
    parser.add_argument(
        "--key-bytes",
        type=int,
        required=True,
        help="Number of bytes for each key (positive integer)",
    )
    parser.add_argument(
        "--value-bytes",
        type=int,
        required=True,
        help="Number of bytes for each value (positive integer)",
    )
    parser.add_argument(
        "--total-size",
        type=str,
        required=True,
        help=(
            "Total output size, accepts suffixes B, KB, MB, GB (e.g., 1024MB, 1GB)"
        ),
    )
    parser.add_argument(
        "--out-dir",
        type=str,
        required=True,
        help="Output directory path",
    )
    parser.add_argument(
        "--base-name",
        type=str,
        default="input.dat",
        help="Base name for the output file (default: records)",
    )
    parser.add_argument(
        "--distribution",
        type=str,
        default="uniform",
        choices=["uniform"],
        help="Record distribution (currently only 'uniform')",
    )

    args = parser.parse_args(argv)

    if args.key_bytes <= 0:
        raise SystemExit("--key-bytes must be > 0")
    if args.value_bytes < 0:
        raise SystemExit("--value-bytes must be >= 0")

    try:
        total_bytes = parse_total_size(args.total_size)
    except ValueError as exc:
        raise SystemExit(str(exc))

    record_size = args.key_bytes + args.value_bytes
    if total_bytes < record_size:
        raise SystemExit(
            "Total size is smaller than a single record. Increase --total-size or reduce record size."
        )

    if args.distribution != "uniform":
        raise SystemExit("Only 'uniform' distribution is implemented at this time.")

    os.makedirs(args.out_dir or ".", exist_ok=True)

    size_suffix = human_size_suffix(total_bytes)
    out_filename = f"{args.base_name}-{size_suffix}.bin"
    out_path = os.path.join(args.out_dir, out_filename)

    records, bytes_written, leftover = write_uniform_records(
        file_path=out_path,
        key_bytes=args.key_bytes,
        value_bytes=args.value_bytes,
        total_bytes=total_bytes,
    )

    # Report summary
    print(
        f"Wrote {records} records ({bytes_written} bytes) to '{out_path}'. Leftover bytes not used: {leftover}.",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))


