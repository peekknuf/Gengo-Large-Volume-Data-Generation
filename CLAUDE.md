# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Gengo is a high-performance Go CLI tool for generating large-scale synthetic relational datasets. It specializes in creating normalized data models (3NF) for e-commerce, financial, and medical domains with optimized performance for multi-gigabyte datasets.

## Common Commands

### Building and Running
```bash
# Build the binary
go build

# Run the interactive data generator
./Gengo gen

# Install dependencies
go mod tidy

# Run tests
go test ./...

# Run benchmarks
go test -bench=. ./tests/
```

### Development Commands
```bash
# Build the binary (run after making changes)
go build

# Format code
go fmt ./...

# Run vet
go vet ./...

# Test specific package
go test ./internal/core/
```

## Architecture Overview

### Core Components

**Main Entry Point**: `main.go:76`
- Uses Cobra CLI framework
- Single `gen` command that orchestrates the entire generation process
- No random seeding - uses system randomness for synthetic data generation

**Core Orchestrator**: `internal/core/orchestrator.go:48`
- Central coordinator for all data generation
- Handles four model types: `ecommerce`, `ecommerce-ds`, `financial`, `medical`
- Implements concurrent generation with goroutines and channels
- Manages dimension → fact dependencies

**Sizing Engine**: `internal/core/sizing.go:143`
- Calculates row counts based on target GB size
- Uses empirical effective size per row estimates
- Maintains realistic ratios between dimensions and facts

### Domain-Specific Architecture

**Models** (`internal/models/`):
- Struct definitions for each domain (ecommerce, financial, medical)
- Include proper JSON/Parquet struct tags
- Follow 3NF normalization principles

**Simulation** (`internal/simulation/`):
- Data generation logic using `brianvoe/gofakeit`
- Implements weighted sampling for realistic distributions
- Optimized for high-performance concurrent generation

**Formats** (`internal/formats/`):
- Multi-format output: CSV, JSON Lines, Apache Parquet
- Uses Apache Arrow/Parquet for efficient columnar storage

### Performance Optimizations

The codebase implements several sophisticated optimizations:

1. **Per-worker RNG**: Eliminates global lock contention in random number generation
2. **Parallel byte chunk formatting**: Workers format data directly into byte slices
3. **Epoch timestamps**: Uses raw Unix timestamps instead of formatted strings
4. **Block allocation**: Reduces atomic operations for ID generation (100k ID blocks)
5. **Slice-based lookups**: Replaces maps with slices for O(1) access in hot paths
6. **Optimized buffers**: 16MB buffers for better throughput

### Concurrent Generation Pattern

The architecture follows a consistent pattern:
1. Generate dimensions concurrently with proper dependency management
2. Extract surrogate keys for foreign key relationships
3. Generate fact tables using optimized concurrent algorithms
4. Write data concurrently with error handling via channels

## Key Files and Structure

### Configuration and Input
- `internal/core/input.go`: Handles interactive user input and model selection
- `internal/core/sizing.go`: Calculates row counts from target size

### Data Models
- `internal/models/ecommerce/ecommerce.go`: E-commerce schema (customers, products, orders)
- `internal/models/financial/financial.go`: Financial schema (companies, exchanges, stock prices)
- `internal/models/medical/medical.go`: Medical schema (patients, doctors, appointments)
- `internal/models/ecommerce-ds/ecommerce-ds.go`: TPC-DS based e-commerce schema

### Generation Logic
- `internal/simulation/*/simulate_dims.go`: Dimension table generation
- `internal/simulation/*/simulate_facts.go`: Fact table generation with optimizations
- `internal/core/orchestrator.go`: Main generation coordinator

### Output Formats
- `internal/formats/`: Multi-format writers (CSV, JSON, Parquet)
- Uses Apache Arrow for Parquet generation with Snappy compression

### Testing
- `tests/benchmark_test.go`: Performance benchmarks
- `tests/e2e_test.go`: End-to-end integration tests

## Development Guidelines

### Adding New Data Models
1. Create model structs in `internal/models/[domain]/`
2. Implement dimension generation in `internal/simulation/[domain]/simulate_dims.go`
3. Implement fact generation in `internal/simulation/[domain]/simulate_facts.go`
4. Add sizing logic in `internal/core/sizing.go`
5. Update orchestrator to handle new model type

### Performance Considerations
- Always use per-worker RNG instances in concurrent code
- Prefer byte slices and `strconv.Append*` for formatting
- Use block allocation for ID generation to reduce atomic contention
- Implement proper dependency management between dimensions and facts
- Profile with `go test -bench=. -cpuprofile=cpu.out` for optimization

### Testing Strategy
- Benchmarks focus on generation speed for large datasets
- End-to-end tests verify complete workflow and output correctness
- Test multiple output formats and data models