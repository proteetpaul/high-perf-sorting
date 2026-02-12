import argparse
import sys
import numpy as np
import matplotlib.pyplot as plt
from collections import defaultdict

"""
Biosnoop output is in the following format:
TIME(s)     COMM           PID     DISK      T SECTOR     BYTES  QUE(ms) LAT(ms)
"""
def parse_biosnoop(file, pid):
    latencies = []
    io_sizes = []
    trace_data = []  # Store trace data: (sector, io_size, queued_time, latency)
    io_events = []  # Store IO events with time: (time_s, io_type, bytes)
    
    for line in file:
        line = line.strip()
        if not line or line.startswith("TIME(") or line.startswith("--"):
            continue
        parts = line.split()
        if len(parts) < 8:
            continue
        try:
            time_s = float(parts[0])  # TIME column (in seconds)
            line_pid = int(parts[2])
            comm = parts[1]
            io_type = parts[4]  # T column: R for read, W for write
            que_ms = float(parts[-2])  # QUE column
            lat_ms = float(parts[-1])  # LAT column
            io_size = int(parts[-3])  # BYTES column
            # Extract sector number - typically in the 6th column
            sector = int(parts[5])
        except (ValueError, IndexError):
            continue
        if line_pid == pid:
            if timing_type == "queued":
                latencies.append(que_ms)
            elif timing_type == "block_device":
                latencies.append(lat_ms)
            elif timing_type == "total":
                latencies.append(lat_ms + que_ms)
            io_sizes.append(io_size)
            # Store trace data: sector, io_size, queued_time, latency
            trace_data.append((sector, io_size, que_ms, lat_ms))
            # Store IO event with time and type for bandwidth calculation
            io_events.append((time_s, io_type, io_size))
    return latencies, io_sizes, trace_data, io_events

def print_percentiles(latencies):
    if not latencies:
        print(f"No I/O events found.")
        return
    percentiles = [50, 70, 90, 99]
    values = np.percentile(latencies, percentiles)
    
    for p, v in zip(percentiles, values):
        print(f"  p{p}: {v:.3f}")
    print(f"Mean: {np.mean(latencies)}")
    print(f"  count: {len(latencies)} events")

def calculate_bandwidth(io_events, interval_ms):
    """
    Calculate read and write bandwidth for each time interval.
    
    Args:
        io_events: List of tuples (time_s, io_type, bytes)
        interval_ms: Time interval in milliseconds
    
    Returns:
        Tuple of (time_intervals, read_bandwidths, write_bandwidths)
        Bandwidths are in MB/s
    """
    if not io_events:
        return [], [], []
    
    # Convert interval from ms to seconds
    interval_s = interval_ms / 1000.0
    
    # Find time range
    times = [event[0] for event in io_events]
    min_time = min(times)
    max_time = max(times)
    
    # Create time bins
    num_intervals = int(np.ceil((max_time - min_time) / interval_s))
    time_intervals = []
    read_bytes = defaultdict(float)
    write_bytes = defaultdict(float)
    
    # Group IO events by time interval
    for time_s, io_type, bytes_count in io_events:
        # Calculate which interval this event belongs to
        interval_idx = int((time_s - min_time) / interval_s)
        interval_start = min_time + interval_idx * interval_s
        
        if interval_start not in time_intervals:
            time_intervals.append(interval_start)
        
        # Accumulate bytes by type
        if io_type.upper() == 'R':
            read_bytes[interval_start] += bytes_count
        elif io_type.upper() == 'W':
            write_bytes[interval_start] += bytes_count
    
    # Sort time intervals
    time_intervals = sorted(set(time_intervals))
    
    # Calculate bandwidth in MB/s for each interval
    read_bandwidths = []
    write_bandwidths = []
    
    for interval_start in time_intervals:
        read_bw = (read_bytes[interval_start] / (1024 * 1024)) / interval_s  # MB/s
        write_bw = (write_bytes[interval_start] / (1024 * 1024)) / interval_s  # MB/s
        read_bandwidths.append(read_bw)
        write_bandwidths.append(write_bw)
    
    return time_intervals, read_bandwidths, write_bandwidths

def plot_bandwidth(time_intervals, read_bandwidths, write_bandwidths, interval_ms, output_file=None, pid=None, input_file=None):
    """
    Plot read and write bandwidth over time.
    
    Args:
        time_intervals: List of interval start times (in seconds)
        read_bandwidths: List of read bandwidths (in MB/s)
        write_bandwidths: List of write bandwidths (in MB/s)
        interval_ms: Time interval in milliseconds (for title)
        output_file: Output file path for the plot (if None, generates default name)
        pid: Process ID (for default filename generation)
        input_file: Input file path (for default filename generation)
    """
    if not time_intervals:
        print("No data to plot.")
        return
    
    plt.figure(figsize=(12, 6))
    
    # Convert time intervals to relative time (seconds from start)
    start_time = time_intervals[0]
    relative_times = [(t - start_time) * 1000 for t in time_intervals]  # Convert to ms
    
    plt.plot(relative_times, read_bandwidths, label='Read', color='blue', linewidth=2)
    plt.plot(relative_times, write_bandwidths, label='Write', color='red', linewidth=2)
    
    plt.xlabel('Time (ms)', fontsize=12)
    plt.ylabel('Bandwidth (MB/s)', fontsize=12)
    plt.title(f'Disk I/O Bandwidth Over Time (Interval: {interval_ms} ms)', fontsize=14)
    plt.legend(fontsize=11)
    plt.grid(True, alpha=0.3)
    
    plt.tight_layout()
    
    # Generate default filename if not provided
    if output_file is None:
        if input_file:
            base_name = input_file.rsplit('.', 1)[0] if '.' in input_file else input_file
            output_file = f"{base_name}_bandwidth_pid{pid}_interval{interval_ms}ms.png"
        else:
            output_file = f"bandwidth_pid{pid}_interval{interval_ms}ms.png"
    
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"Plot saved to {output_file}")
    plt.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Parse biosnoop-bpfcc output and plot bandwidth over time.")
    parser.add_argument("--pid", type=int, required=True, help="PID to filter")
    parser.add_argument("--file", type=str, help="Path to biosnoop output file (default: stdin)")
    parser.add_argument("--interval", type=float, required=True, help="Time interval in milliseconds for bandwidth calculation")
    parser.add_argument("--output", type=str, help="Output file path for the plot (default: auto-generated based on input file and PID)")
    parser.add_argument("--timing-type", type=str, default="total", choices=["queued", "block_device", "total"],
                       help="Type of latency to calculate (default: total)")
    args = parser.parse_args()

    global timing_type
    timing_type = args.timing_type

    if args.file:
        with open(args.file, "r") as f:
            latencies, io_sizes, trace_data, io_events = parse_biosnoop(f, args.pid)
    else:
        latencies, io_sizes, trace_data, io_events = parse_biosnoop(sys.stdin, args.pid)

    # Calculate and plot bandwidth
    if io_events:
        time_intervals, read_bandwidths, write_bandwidths = calculate_bandwidth(io_events, args.interval)
        plot_bandwidth(time_intervals, read_bandwidths, write_bandwidths, args.interval, args.output, args.pid, args.file)
    else:
        print("No I/O events found for plotting.")

    # Print latency statistics if available
    if latencies:
        print(f"\n{args.timing_type.replace('_', ' ').title()} latency percentiles (ms):")
        print_percentiles(latencies)

        print(f"\nIO sizes percentiles:")
        print_percentiles(io_sizes)