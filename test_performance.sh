#!/bin/bash

# Enhanced performance test for Gengo with multiple configuration options
# Usage: ./test_performance.sh [buffer_size_mb] [worker_count] [target_size_gb]

set -e

# Default values (based on optimal test results)
DEFAULT_BUFFER_MB=8
DEFAULT_WORKERS=4
DEFAULT_TARGET_GB=1

# Parse command line arguments
BUFFER_MB=${1:-$DEFAULT_BUFFER_MB}
WORKERS=${2:-$DEFAULT_WORKERS}
TARGET_GB=${3:-$DEFAULT_TARGET_GB}

# Convert MB to KB for environment variable
BUFFER_KB=$((BUFFER_MB * 1024))

echo "=========================================="
echo "Gengo Performance Test"
echo "=========================================="

# Check if Go is installed
if ! command -v go &> /dev/null; then
    echo "Error: Go is not installed"
    exit 1
fi

# Check system resources
CPU_CORES=$(nproc)
MEM_GB=$(free -g | awk 'NR==2{printf "%.1f", $2}')
DISK_GB=$(df -h /tmp | awk 'NR==2{print $4}' | sed 's/G//')

echo "System Information:"
echo "CPU Cores: $CPU_CORES"
echo "Memory: ${MEM_GB}GB"
echo "Available Disk: ${DISK_GB}GB"
echo ""

# Validate inputs
if [[ ! "$BUFFER_MB" =~ ^[0-9]+$ ]] || [ "$BUFFER_MB" -lt 1 ] || [ "$BUFFER_MB" -gt 256 ]; then
    echo "Error: Buffer size must be between 1-256 MB"
    exit 1
fi

if [[ ! "$WORKERS" =~ ^[0-9]+$ ]] || [ "$WORKERS" -lt 1 ] || [ "$WORKERS" -gt 32 ]; then
    echo "Error: Worker count must be between 1-32"
    exit 1
fi

if [[ ! "$TARGET_GB" =~ ^[0-9]+(\.[0-9]+)?$ ]] || [ "$(echo "$TARGET_GB < 0.1" | bc)" -eq 1 ] || [ "$(echo "$TARGET_GB > 10" | bc)" -eq 1 ]; then
    echo "Error: Target size must be between 0.1-10 GB"
    exit 1
fi

# Check if sufficient disk space
if [ "$DISK_GB" -lt "$((TARGET_GB + 1))" ]; then
    echo "Warning: Low disk space (${DISK_GB}GB available, need ~$((TARGET_GB + 1))GB)"
fi

# Build Gengo
echo "Building Gengo..."
go build -o Gengo .

if [ ! -f "Gengo" ]; then
    echo "Error: Failed to build Gengo"
    exit 1
fi

echo "Build successful!"
echo ""

# Set environment variables
export GENGO_BUFFER_SIZE=$BUFFER_KB
export GENGO_WORKER_COUNT=$WORKERS

echo "Running performance test with configuration:"
echo "- Buffer Size: ${BUFFER_MB}MB"
echo "- Worker Count: $WORKERS"
echo "- Target Size: ${TARGET_GB}GB"
echo ""

# Time the execution
START_TIME=$(date +%s.%N)

# Run Gengo with specified settings
echo "Starting data generation..."
echo "ecommerce-ds
$TARGET_GB
csv
/tmp/gengo_performance_test" | ./Gengo gen

END_TIME=$(date +%s.%N)

# Calculate duration
DURATION=$(echo "$END_TIME - $START_TIME" | bc)

# Calculate output size
if [ -d "/tmp/gengo_performance_test" ]; then
    OUTPUT_SIZE=$(du -sh /tmp/gengo_performance_test | cut -f1)
    OUTPUT_SIZE_BYTES=$(du -sb /tmp/gengo_performance_test | cut -f1)
else
    OUTPUT_SIZE="Unknown"
    OUTPUT_SIZE_BYTES=0
fi

# Display results
echo ""
echo "=========================================="
echo "PERFORMANCE TEST RESULTS"
echo "=========================================="
echo "Configuration: ${BUFFER_MB}MB buffer, $WORKERS workers"
echo "Target Size: ${TARGET_GB}GB"
echo "Actual Output: $OUTPUT_SIZE"
echo "Duration: $DURATION seconds"

# Calculate throughput if we have duration and output size
if [ "$DURATION" != "0" ] && [ "$OUTPUT_SIZE_BYTES" -gt 0 ]; then
    THROUGHput_MB=$(echo "scale=2; $OUTPUT_SIZE_BYTES / 1024 / 1024 / $DURATION" | bc)
    echo "Throughput: $THROUGHput_MB MB/s"
fi

echo ""
echo "Test completed successfully!"

# Cleanup
echo "Cleaning up test files..."
rm -rf /tmp/gengo_performance_test

echo ""
echo "=========================================="
echo "CONFIGURATION TESTED:"
echo "- Buffer Size: ${BUFFER_MB}MB"
echo "- Worker Count: $WORKERS"
echo "- System CPU Cores: $CPU_CORES"
echo "- System Memory: ${MEM_GB}GB"
echo "=========================================="

# Performance recommendations
echo ""
echo "PERFORMANCE RECOMMENDATIONS:"
echo "============================"

# Buffer size recommendations
if [ "$BUFFER_MB" -lt 4 ]; then
    echo "💡 Try larger buffer sizes (4-16MB) for better performance"
elif [ "$BUFFER_MB" -gt 32 ]; then
    echo "💡 Try smaller buffer sizes (4-16MB) to reduce memory usage"
fi

# Worker count recommendations
if [ "$WORKERS" -lt "$CPU_CORES" ]; then
    echo "💡 Try more workers (up to $CPU_CORES) for better CPU utilization"
elif [ "$WORKERS" -gt "$((CPU_CORES * 2))" ]; then
    echo "💡 Try fewer workers (around $CPU_CORES) to reduce contention"
fi

# Show optimal configuration based on our tests
echo ""
echo "BASED ON OUR TESTING:"
echo "- Optimal: 8MB buffer, 4 workers (~250 MB/s)"
echo "- Fastest: 16MB buffer, 4 workers (~244 MB/s)"
echo "- Conservative: 4MB buffer, 2 workers (~200 MB/s)"
echo "=========================================="