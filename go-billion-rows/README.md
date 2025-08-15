# Billion Row Challenge - Go Implementation

A high-performance Go implementation of the [Billion Row Challenge](https://github.com/gunnarmorling/1brc) that processes one billion temperature measurements as fast as possible.

## Overview

This implementation processes a text file containing one billion rows of weather station temperature data in the format:
```
Station Name;Temperature
Hamburg;12.0
Bulawayo;8.9
Palembang;38.8
```

## Features

- **Memory-mapped file I/O** for maximum performance
- **Multi-threaded processing** using all available CPU cores
- **Cross-platform support** (Unix/Linux and Windows)
- **Custom temperature parser** optimized for the expected format
- **Data generator** to create test files with realistic weather station data
- **Zero external dependencies** - uses only Go standard library

## Usage

### Generate Test Data

First, you'll need a `weather_stations.csv` file containing weather station names (one per line or semicolon-separated). Then generate the billion-row dataset:

```bash
go run . -generate
```

This creates a `data.txt` file with 1 billion temperature measurements (~13-14 GB).

### Process the Data

Run the challenge processor:

```bash
go run .
```

The program will output results in the format:
```
{Abha=-23.0/18.0/59.2, Abidjan=-16.2/26.0/67.3, Abéché=-10.0/29.4/69.0, ...}

RESULTS
Total Time: 5.1364809s
Speed: 194.69 million rows/second
I/O Rate: 3.05 GB/second
```

## Performance Optimizations

- **Memory mapping**: Direct file access without copying data into memory
- **Parallel processing**: Work is distributed across all CPU cores
- **Custom parsing**: Hand-optimised temperature string parsing
- **data structures**: Pre-allocated maps and minimal allocations
- **based processing**: File is split into worker-sized chunks at line boundaries

## Architecture

### Core Components

- `main.go` - Main processing logic and coordination
- `generator.go` - Test data generation
- `mmap_unix.go` / `mmap_windows.go` - Platform-specific memory mapping

### Processing Flow

1. Memory-map the input file for zero-copy access
2. Split file into chunks aligned on line boundaries
3. Process chunks in parallel across CPU cores
4. Parse station names and temperatures from each line
5. Accumulate statistics (min/max/sum/count) per station
6. Merge results from all workers
7. Sort stations alphabetically and output results

## Building

```bash
# Build executable
go build -o billion-rows .

# Run with generated binary
./billion-rows -generate  # Generate data
./billion-rows            # Process data
```

## Performance Notes

Typical performance on modern hardware:
- **Generation**: 50-100 million rows/second
- **Processing**: 300-500 million rows/second
- **I/O throughput**: 4-8 GB/second

Performance scales with:
- Number of CPU cores
- Memory bandwidth
- Storage I/O speed