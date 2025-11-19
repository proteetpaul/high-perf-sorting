#!/bin/bash

# Default values
FILE_SIZE="1M"
KEY_SIZE=8
VALUE_SIZE=8
MEMORY_SIZE="100M"
READ_CHUNK_SIZE="100M"
NUM_THREADS=1
WORKING_DIR=$(realpath ".")
ENABLE_PROFILE=false
PROFILE_OUTPUT="perf.data"
FLAMEGRAPH_OUTPUT="flamegraph.svg"
SEPARATE_VALUES=false
ENABLE_MEMORY_PROFILING=false
PCM_MEMORY="/users/proteet/pcm/build/bin/pcm-memory"

# Function to show usage
show_usage() {
    echo "Usage: $0 [OPTIONS]"
    echo "Options:"
    echo "  --file-size SIZE           Input file size (default: 1M)"
    echo "  --key-size SIZE            Key size in bytes (default: 8)"
    echo "  --value-size SIZE          Value size in bytes (default: 8)"
    echo "  --memory-size SIZE         Memory size (default: 100M)"
    echo "  --num-threads COUNT        Number of threads for parallel sorting (default: 1)"
    echo "  --working-dir DIR          Working directory (default: .)"
    echo "  --profile                  Enable perf profiling"
    echo "  --profile-output FILE      Perf output file (default: perf.data)"
    echo "  --flamegraph-output FILE   Flamegraph output file (default: flamegraph.svg)"
    echo "  --help, -h                 Show this help message"
    echo ""
    echo "Size units: B (bytes), K (KB), M (MB), G (GB)"
    echo "Examples: 1M, 512K, 2G, 1024B"
    echo ""
    echo "Examples:"
    echo "  $0 --file-size 2M --memory-size 512M"
    echo "  $0 --file-size 1G --profile --flamegraph-output my_flamegraph.svg"
    echo "  $0 --key-size 10 --value-size 90 --working-dir /tmp"
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --file-size)
            FILE_SIZE="$2"
            shift 2
            ;;
        --key-size)
            KEY_SIZE="$2"
            shift 2
            ;;
        --value-size)
            VALUE_SIZE="$2"
            shift 2
            ;;
        --memory-size)
            MEMORY_SIZE="$2"
            shift 2
            ;;
        --num-threads)
            NUM_THREADS="$2"
            shift 2
            ;;
        --working-dir)
            WORKING_DIR="$2"
            # Convert to absolute path
            WORKING_DIR=$(realpath "$WORKING_DIR")
            shift 2
            ;;
        --profile)
            ENABLE_PROFILE=true
            shift
            ;;
        --profile-output)
            PROFILE_OUTPUT="$2"
            shift 2
            ;;
        --separate-values)
            SEPARATE_VALUES=true
            shift
            ;;
        --enable-mem-profiling)
            ENABLE_MEMORY_PROFILING=true
            shift
            ;;
        --flamegraph-output)
            FLAMEGRAPH_OUTPUT="$2"
            shift 2
            ;;
        --help|-h)
            show_usage
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            show_usage
            exit 1
            ;;
    esac
done

ROOT_DIR=$(pwd)
BUILD_DIR=$ROOT_DIR/build

# Check if the build directory exists, if not create it
if [ ! -d "$BUILD_DIR" ]; then
    mkdir -p "$BUILD_DIR"
fi

# Check if working directory exists, if not create it
if [ ! -d "$WORKING_DIR" ]; then
    mkdir -p "$WORKING_DIR"
fi

cd $BUILD_DIR
cmake --build .

if [ $? -ne 0 ]; then
    echo "Build failed!"
    exit 1
fi

# Generate human-friendly file names
# Convert file size to a format suitable for filenames (replace non-alphanumeric chars)
FILE_SIZE_CLEAN=$(echo "$FILE_SIZE" | sed 's/[^a-zA-Z0-9]/-/g')
INPUT_FILE="$WORKING_DIR/input-$FILE_SIZE_CLEAN.dat"
OUTPUT_FILE="$WORKING_DIR/output-$FILE_SIZE_CLEAN.dat"

# Generate test data if it doesn't exist
if [ ! -f "$INPUT_FILE" ]; then
    echo "Generating test data..."
    python3 $ROOT_DIR/tools/generate_records.py \
        --output "$INPUT_FILE" \
        --total-size "$FILE_SIZE" \
        --key-bytes "$KEY_SIZE" \
        --value-bytes "$VALUE_SIZE"
    
    if [ $? -ne 0 ]; then
        echo "Failed to generate test data!"
        exit 1
    fi
fi

# Prepare command arguments
CMD_ARGS="--file-size $FILE_SIZE --key-size $KEY_SIZE --value-size $VALUE_SIZE"
CMD_ARGS="$CMD_ARGS --memory-size $MEMORY_SIZE"
CMD_ARGS="$CMD_ARGS --num-threads $NUM_THREADS --working-dir $WORKING_DIR"

if [ "$SEPARATE_VALUES" = true ]; then
    CMD_ARGS="$CMD_ARGS --separate-values"
fi

echo "Running sorter with parameters:"
echo "  File size: $FILE_SIZE"
echo "  Key size: $KEY_SIZE"
echo "  Value size: $VALUE_SIZE"
echo "  Memory size: $MEMORY_SIZE"
echo "  Read chunk size: $READ_CHUNK_SIZE"
echo "  Number of threads: $NUM_THREADS"
echo "  Working directory: $WORKING_DIR"
echo ""

if [ "$ENABLE_MEMORY_PROFILING" = true ]; then
    echo "Running with memory bw monitor..."
    echo ""
    sudo $PCM_MEMORY 0.02 -nc -silent -csv=memory_bw_output.csv -- ./src/sorter $CMD_ARGS
    python3 ../tools/plot_mem_bw.py memory_bw_output.csv
    echo "Exiting"
    exit 0
fi
# Run the sorter
if [ "$ENABLE_PROFILE" = true ]; then
    echo "Running with perf profiling..."
    echo "Profile output: $PROFILE_OUTPUT"
    echo "Flamegraph output: $FLAMEGRAPH_OUTPUT"
    echo ""
    
    # Check if perf is available
    if ! command -v perf &> /dev/null; then
        echo "Error: perf is not installed. Please install it first:"
        echo "  sudo apt-get install linux-tools-common linux-tools-generic"
        exit 1
    fi
    
    # Run with perf
    perf record -g -o "$PROFILE_OUTPUT" ./src/sorter $CMD_ARGS
    
    if [ $? -eq 0 ]; then        
        # Generate flamegraph using local FlameGraph scripts
        echo "Generating flamegraph using local FlameGraph scripts..."
        FLAMEGRAPH_DIR="$ROOT_DIR/FlameGraph"
        
        if [ -f "$FLAMEGRAPH_DIR/flamegraph.pl" ] && [ -f "$FLAMEGRAPH_DIR/stackcollapse-perf.pl" ]; then
            perf script -i "$PROFILE_OUTPUT" | "$FLAMEGRAPH_DIR/stackcollapse-perf.pl" | "$FLAMEGRAPH_DIR/flamegraph.pl" > "$FLAMEGRAPH_OUTPUT"
            echo "Flamegraph saved to: $FLAMEGRAPH_OUTPUT"
        else
            echo "Error: Local FlameGraph scripts not found in $FLAMEGRAPH_DIR"
        fi
    else
        echo "Sorter failed!"
        exit 1
    fi
else
    echo "Running sorter..."
    ./src/sorter $CMD_ARGS
    
    if [ $? -eq 0 ]; then
        echo "Sorter completed successfully!"
        
        # Verify the output
        if [ -f "$OUTPUT_FILE" ]; then
            echo "Verifying sorted output..."
            python3 $ROOT_DIR/tools/verify_sort.py "$OUTPUT_FILE" "$KEY_SIZE" "$VALUE_SIZE"
        fi
    else
        echo "Sorter failed!"
        exit 1
    fi
fi