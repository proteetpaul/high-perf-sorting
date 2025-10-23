#!/usr/bin/env python3
"""
Script to verify if a sorted output file is correctly sorted.
Takes output file name, key size, and value size as arguments.
"""

import sys
import os
import struct
import argparse


def read_record(file, key_size, value_size):
    """Read a single record from the file."""
    record_size = key_size + value_size
    data = file.read(record_size)
    if len(data) != record_size:
        return None
    return data


def verify_sorting(output_file, key_size, value_size):
    """Verify if the output file is correctly sorted."""
    if not os.path.exists(output_file):
        print(f"Error: Output file '{output_file}' does not exist.")
        return False
    
    file_size = os.path.getsize(output_file)
    record_size = key_size + value_size
    
    if file_size % record_size != 0:
        print(f"Error: File size ({file_size} bytes) is not divisible by record size ({record_size} bytes).")
        print(f"This suggests the file is corrupted or has incomplete records.")
        return False
    
    total_records = file_size // record_size
    print(f"Verifying {total_records} records...")
    
    try:
        with open(output_file, 'rb') as f:
            prev_record = read_record(f, key_size, value_size)
            if prev_record is None:
                print("Error: Could not read first record.")
                return False
            
            record_count = 1
            while True:
                current_record = read_record(f, key_size, value_size)
                if current_record is None:
                    break
                
                record_count += 1
                
                # Compare keys (first key_size bytes)
                prev_key = prev_record[:key_size]
                current_key = current_record[:key_size]
                
                if current_key < prev_key:
                    print(f"Error: Records are not sorted!")
                    print(f"Record {record_count-1} key: {prev_key.hex()}")
                    print(f"Record {record_count} key: {current_key.hex()}")
                    return False
                
                prev_record = current_record
    
    except IOError as e:
        print(f"Error reading file: {e}")
        return False
    
    print(f"✓ Successfully verified {total_records} records are sorted!")
    return True


def main():
    parser = argparse.ArgumentParser(description='Verify if a sorted output file is correctly sorted.')
    parser.add_argument('output_file', help='Path to the output file to verify')
    parser.add_argument('key_size', type=int, help='Size of the key in bytes')
    parser.add_argument('value_size', type=int, help='Size of the value in bytes')
    
    args = parser.parse_args()
    
    if args.key_size <= 0 or args.value_size <= 0:
        print("Error: Key size and value size must be positive integers.")
        sys.exit(1)
    
    print(f"Verifying file: {args.output_file}")
    print(f"Key size: {args.key_size} bytes")
    print(f"Value size: {args.value_size} bytes")
    print(f"Record size: {args.key_size + args.value_size} bytes")
    print()
    
    success = verify_sorting(args.output_file, args.key_size, args.value_size)
    
    if success:
        print("✓ File is correctly sorted!")
        sys.exit(0)
    else:
        print("✗ File is NOT correctly sorted!")
        sys.exit(1)


if __name__ == "__main__":
    main()
