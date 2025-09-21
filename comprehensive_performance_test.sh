#!/bin/bash

# Comprehensive Performance Test for Gengo
# Tests multiple configurations sequentially with 5GB target size
# All tests run automatically one after another

set -e

echo "=================================================================="
echo "GENGO COMPREHENSIVE PERFORMANCE TEST"
echo "=================================================================="

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

# Check if sufficient disk space for all tests (5 tests x 5GB = ~25GB + overhead)
if [ "$DISK_GB" -lt 30 ]; then
    echo "Error: Insufficient disk space. Need at least 30GB available for all tests."
    echo "Available: ${DISK_GB}GB"
    exit 1
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

# Test configurations - will run 5GB tests sequentially
configs=(
    "4 2 5"      # Conservative: 4MB buffer, 2 workers, 5GB
    "8 4 5"      # Optimal: 8MB buffer, 4 workers, 5GB
    "16 4 5"     # Fast: 16MB buffer, 4 workers, 5GB
    "16 8 5"     # High performance: 16MB buffer, 8 workers, 5GB
    "32 8 5"     # Maximum: 32MB buffer, 8 workers, 5GB
)

# Array to store results
declare -a results
total_tests=${#configs[@]}
current_test=0

echo "=================================================================="
echo "RUNNING $total_tests PERFORMANCE TESTS (5GB each)"
echo "=================================================================="
echo "This will test 5 different configurations sequentially."
echo "Each test generates 5GB of data."
echo "Estimated total time: 20-40 minutes"
echo ""

# Run each configuration
for config in "${configs[@]}"; do
    current_test=$((current_test + 1))

    # Parse configuration
    read -r buffer_mb workers target_gb <<< "$config"

    echo "=================================================================="
    echo "TEST $current_test/$total_tests: ${buffer_mb}MB buffer, $workers workers, ${target_gb}GB target"
    echo "=================================================================="

    # Convert MB to KB for environment variable
    buffer_kb=$((buffer_mb * 1024))

    # Set environment variables
    export GENGO_BUFFER_SIZE=$buffer_kb
    export GENGO_WORKER_COUNT=$workers

    # Create unique output directory for this test
    output_dir="/tmp/gengo_test_${buffer_mb}mb_${workers}workers_$(date +%s)"

    echo "Starting data generation..."
    echo "Configuration: ${buffer_mb}MB buffer, $workers workers"
    echo "Target: ${target_gb}GB"
    echo "Output directory: $output_dir"
    echo ""

    # Time the execution
    start_time=$(date +%s.%N)

    # Run Gengo with specified settings
    if echo "ecommerce-ds
$target_gb
csv
$output_dir" | timeout 600 ./Gengo gen; then
        end_time=$(date +%s.%N)

        # Calculate duration
        duration=$(echo "$end_time - $start_time" | bc)

        # Calculate output size
        if [ -d "$output_dir" ]; then
            output_size_bytes=$(du -sb "$output_dir" | cut -f1)
            output_size_mb=$(echo "scale=1; $output_size_bytes / 1024 / 1024" | bc)
            throughput=$(echo "scale=2; $output_size_bytes / 1024 / 1024 / $duration" | bc)
            status="SUCCESS"
        else
            output_size_mb="0"
            throughput="0"
            status="FAILED"
        fi

        # Store results
        results+=("$buffer_mb|$workers|$duration|$output_size_mb|$throughput|$status")

        echo "✅ Test completed successfully!"
        echo "Duration: $duration seconds"
        echo "Output: ${output_size_mb}MB"
        echo "Throughput: $throughput MB/s"

    else
        end_time=$(date +%s.%N)
        duration=$(echo "$end_time - $start_time" | bc)
        results+=("$buffer_mb|$workers|$duration|0|0|FAILED")
        echo "❌ Test failed!"
        echo "Duration: $duration seconds"
    fi

    # Cleanup this test's output directory
    rm -rf "$output_dir"

    echo ""
    echo "Progress: $current_test/$total_tests tests completed"

    # Brief pause between tests
    if [ $current_test -lt $total_tests ]; then
        echo "Pausing for 5 seconds before next test..."
        sleep 5
    fi

    echo ""
done

echo "=================================================================="
echo "ALL TESTS COMPLETED - FINAL RESULTS"
echo "=================================================================="

# Display all results
echo ""
echo "Detailed Results:"
echo "Buffer(MB) | Workers | Duration(s) | Output(MB) | Throughput(MB/s) | Status"
echo "--------------------------------------------------------------------------"

for result in "${results[@]}"; do
    IFS='|' read -r buffer workers duration output_mb throughput status <<< "$result"
    printf "%-10d | %-7d | %-11.1f | %-10s | %-16.2f | %s\n" \
           "$buffer" "$workers" "$duration" "${output_mb}" "$throughput" "$status"
done

# Find best performing configuration
best_throughput=0
best_config=""

echo ""
echo "Performance Analysis:"
echo "====================="

for result in "${results[@]}"; do
    IFS='|' read -r buffer workers duration output_mb throughput status <<< "$result"

    if [ "$status" = "SUCCESS" ] && (( $(echo "$throughput > $best_throughput" | bc -l) )); then
        best_throughput=$throughput
        best_config="${buffer}MB buffer, $workers workers"
    fi

    # Analyze each configuration
    echo ""
    echo "Configuration: ${buffer}MB buffer, $workers workers"
    echo "  Status: $status"

    if [ "$status" = "SUCCESS" ]; then
        echo "  Duration: $duration seconds"
        echo "  Throughput: $throughput MB/s"

        # Provide recommendations
        if (( $(echo "$throughput < 100" | bc -l) )); then
            echo "  📊 Performance: SLOW (< 100 MB/s)"
            echo "  💡 Recommendation: Increase buffer size or check system resources"
        elif (( $(echo "$throughput < 200" | bc -l) )); then
            echo "  📊 Performance: GOOD (100-200 MB/s)"
        elif (( $(echo "$throughput < 250" | bc -l) )); then
            echo "  📊 Performance: EXCELLENT (200-250 MB/s)"
        else
            echo "  📊 Performance: OUTSTANDING (> 250 MB/s)"
        fi
    else
        echo "  ⚠️  Test failed - check logs for details"
    fi
done

echo ""
echo "=================================================================="
echo "FINAL RECOMMENDATIONS"
echo "=================================================================="

if [ -n "$best_config" ]; then
    echo "🏆 BEST PERFORMING CONFIGURATION:"
    echo "   Configuration: $best_config"
    echo "   Throughput: $best_throughput MB/s"
    echo ""

    echo "📋 CONFIGURATION RATINGS:"

    # Rank configurations by performance
    declare -a ranked_configs
    for result in "${results[@]}"; do
        IFS='|' read -r buffer workers duration output_mb throughput status <<< "$result"
        if [ "$status" = "SUCCESS" ]; then
            ranked_configs+=("$throughput|$buffer|$workers")
        fi
    done

    # Sort by throughput (descending)
    IFS=$'\n' sorted_configs=($(sort -nr <<<"${ranked_configs[*]}"))
    unset IFS

    rank=1
    for config in "${sorted_configs[@]}"; do
        IFS='|' read -r throughput buffer workers <<< "$config"
        case $rank in
            1) echo "   🥇 $rank. ${buffer}MB buffer, $workers workers: $throughput MB/s" ;;
            2) echo "   🥈 $rank. ${buffer}MB buffer, $workers workers: $throughput MB/s" ;;
            3) echo "   🥉 $rank. ${buffer}MB buffer, $workers workers: $throughput MB/s" ;;
            *) echo "   $rank. ${buffer}MB buffer, $workers workers: $throughput MB/s" ;;
        esac
        rank=$((rank + 1))
    done

else
    echo "❌ All tests failed! Check system configuration and logs."
fi

echo ""
echo "🔧 SYSTEM-SPECIFIC RECOMMENDATIONS:"
echo "   Based on your $CPU_CORES CPU cores and ${MEM_GB}GB RAM:"

if [ "$CPU_CORES" -le 2 ]; then
    echo "   - Use 1-2 workers maximum"
    echo "   - Try 4-8MB buffer sizes"
elif [ "$CPU_CORES" -le 4 ]; then
    echo "   - Use 4 workers (optimal for your system)"
    echo "   - Try 8-16MB buffer sizes"
else
    echo "   - Use 4-8 workers"
    echo "   - Try 16-32MB buffer sizes"
fi

if (( $(echo "$MEM_GB < 8" | bc -l) )); then
    echo "   - Use smaller buffer sizes (4-8MB) to conserve memory"
else
    echo "   - You can use larger buffer sizes (16-32MB)"
fi

echo ""
echo "📝 NOTES:"
echo "   - All tests used 5GB target size for consistent comparison"
echo "   - Tests run sequentially to avoid system overload"
echo "   - Results may vary based on storage type (SSD vs HDD)"
echo "   - Background processes may affect performance"
echo ""

echo "=================================================================="
echo "PERFORMANCE TEST COMPLETE"
echo "=================================================================="

# Final cleanup
echo "Cleaning up temporary files..."
rm -f Gengo

echo "All test files have been cleaned up."
echo ""
echo "To run individual tests:"
echo "  ./comprehensive_performance_test.sh"
echo ""