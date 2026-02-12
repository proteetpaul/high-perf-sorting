import argparse
import sys
import re
import matplotlib.pyplot as plt

"""
Parse bpftrace output and plot read/write bandwidth over time.

bpftrace output format:
<seconds>.<milliseconds>: Read: <MB/s> MB/s, Write: <MB/s> MB/s
Example: 1.234: Read: 100 MB/s, Write: 50 MB/s
"""

def parse_bpftrace(file):
    """
    Parse bpftrace bandwidth output.
    
    Args:
        file: File object or stdin
    
    Returns:
        Tuple of (times, read_bandwidths, write_bandwidths)
        times are in seconds, bandwidths are in MB/s
    """
    times = []
    read_bandwidths = []
    write_bandwidths = []
    
    # Pattern to match: "1.234: Read: 100 MB/s, Write: 50 MB/s"
    # bpftrace uses %d.%03d format, so milliseconds are always 3 digits
    pattern = r'(\d+)\.(\d{3}):\s+Read:\s+(\d+)\s+MB/s,\s+Write:\s+(\d+)\s+MB/s'
    
    for line in file:
        line = line.strip()
        if not line or line.startswith("Tracing complete"):
            continue
        
        match = re.match(pattern, line)
        if match:
            time_sec = int(match.group(1))
            time_ms = int(match.group(2))
            read_bw = int(match.group(3))
            write_bw = int(match.group(4))
            
            # Convert to seconds (e.g., 1.234 -> 1.234 seconds)
            time_total = time_sec + time_ms / 1000.0
            
            times.append(time_total)
            read_bandwidths.append(read_bw)
            write_bandwidths.append(write_bw)
    
    return times, read_bandwidths, write_bandwidths

def plot_bandwidth(times, read_bandwidths, write_bandwidths, output_file=None, input_file=None, start_time=None, end_time=None):
    """
    Plot read and write bandwidth over time.
    
    Args:
        times: List of timestamps (in seconds)
        read_bandwidths: List of read bandwidths (in MB/s)
        write_bandwidths: List of write bandwidths (in MB/s)
        output_file: Output file path for the plot (if None, generates default name)
        input_file: Input file path (for default filename generation)
        start_time: Start time in seconds to clip the plot (None = no clipping)
        end_time: End time in seconds to clip the plot (None = no clipping)
    """
    if not times:
        print("No data to plot.")
        return
    
    # Filter data based on start_time and end_time
    filtered_times = []
    filtered_read = []
    filtered_write = []
    
    for i, t in enumerate(times):
        if start_time is not None and t < start_time:
            continue
        if end_time is not None and t > end_time:
            continue
        filtered_times.append(t)
        filtered_read.append(read_bandwidths[i])
        filtered_write.append(write_bandwidths[i])
    
    if not filtered_times:
        print("No data to plot after clipping.")
        return
    
    # Calculate total bandwidth (read + write)
    filtered_total = [r + w for r, w in zip(filtered_read, filtered_write)]
    
    plt.figure(figsize=(12, 6))
    
    # Convert times to relative time (milliseconds from start)
    plot_start_time = filtered_times[0]
    relative_times = [(t - plot_start_time) * 1000 for t in filtered_times]  # Convert to ms
    
    # Plot total bandwidth first with dashed line and lower z-order so read/write are on top
    plt.plot(relative_times, filtered_total, label='Total', color='green', linewidth=1, 
             linestyle='--', alpha=0.7, zorder=1)
    # Plot read and write on top with higher z-order
    plt.plot(relative_times, filtered_read, label='Read', color='blue', linewidth=1, zorder=2)
    plt.plot(relative_times, filtered_write, label='Write', color='red', linewidth=1, zorder=2)
    
    plt.xlabel('Time (ms)', fontsize=12)
    plt.ylabel('Bandwidth (MB/s)', fontsize=12)
    plt.title('Disk I/O Bandwidth Over Time (from bpftrace)', fontsize=14)
    plt.legend(fontsize=11)
    plt.grid(True, alpha=0.3)
    
    plt.tight_layout()
    
    # Generate default filename if not provided
    if output_file is None:
        if input_file:
            base_name = input_file.rsplit('.', 1)[0] if '.' in input_file else input_file
            output_file = f"{base_name}_bandwidth.png"
        else:
            output_file = "bpftrace_bandwidth.png"
    
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"Plot saved to {output_file}")
    plt.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Parse bpftrace bandwidth output and plot read/write bandwidth over time.")
    parser.add_argument("--file", type=str, help="Path to bpftrace output file (default: stdin)")
    parser.add_argument("--output", type=str, help="Output file path for the plot (default: auto-generated based on input file)")
    parser.add_argument("--start", type=float, help="Start time in seconds to clip the plot (default: no clipping)")
    parser.add_argument("--end", type=float, help="End time in seconds to clip the plot (default: no clipping)")
    args = parser.parse_args()

    if args.file:
        with open(args.file, "r") as f:
            times, read_bandwidths, write_bandwidths = parse_bpftrace(f)
    else:
        times, read_bandwidths, write_bandwidths = parse_bpftrace(sys.stdin)

    if times:
        original_count = len(times)
        plot_bandwidth(times, read_bandwidths, write_bandwidths, args.output, args.file, args.start, args.end)
        if args.start is not None or args.end is not None:
            # Count filtered points
            filtered_count = sum(1 for t in times 
                               if (args.start is None or t >= args.start) and 
                                  (args.end is None or t <= args.end))
            print(f"Processed {original_count} data points ({filtered_count} after clipping)")
        else:
            print(f"Processed {original_count} data points")
    else:
        print("No bandwidth data found in input.")
