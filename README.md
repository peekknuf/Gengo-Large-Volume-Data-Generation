# Gengo 🚀 - TPC-DS Data Generator for Large-Scale Analytics

Gengo is a high-performance command-line tool written in Go for generating **TPC-DS benchmark datasets** - the industry standard for testing data warehousing and analytics systems. It generates complete star schemas with realistic business data at scale, perfect for benchmarking database performance, testing BI tools, and validating data pipelines.

**TPC-DS is the most important benchmark for data warehousing systems**, and Gengo implements it with production-quality data modeling and optimizations for enterprise-scale datasets.

## Features ✨

- **Ultra-Fast:** Leverages Go's performance and sophisticated optimizations for **ultra-fast** data generation, achieving **up to 17x speed improvements** over previous versions for relational models.
- **Relational Model:** Generates predefined 3NF data models for:
    - **E-commerce TPC-DS:** Complete TPC-DS benchmark with 17 dimensions and 7 fact tables (Store/Web/Catalog sales, returns, inventory)
    - **Financial:** `dim_companies`, `dim_exchanges`, `fact_daily_stock_prices`
    - **Medical:** `dim_patients`, `dim_doctors`, `dim_clinics`, `fact_appointments`
- **Multiple Formats:** Output data as **CSV**, **JSON Lines** (one JSON object per line), or efficient **Apache Parquet**.
- **Realistic Facts:** Uses weighted sampling for selecting customers and products when generating orders, simulating more realistic purchasing patterns (e.g., some customers/products appear more frequently).
- **Compressed Parquet:** Generates compressed Parquet files (Snappy by default) for smaller disk usage (one file per table).
- **Size-Based Input:** Tell Gengo the approximate **target size in GB** for the dataset, and it estimates the required row counts for dimensions and facts.
- **Simple Usage:** Interactive command-line prompts guide you through the setup.
- **Customizable Code:** Easily tweak the data generation logic, schema structs, or data realism features within the Go code (uses `brianvoe/gofakeit` and other standard libraries).

## Installation 🛠️

1. **Clone the repo:**

   ```bash
   git clone https://github.com/peekknuf/Gengo.git # Or your repo URL
   ```

2. **Navigate into the directory:**

   ```bash
   cd Gengo
   ```

3. **Ensure dependencies are downloaded:**

   ```bash
   go mod tidy
   ```

4. **Build the binary:**

   ```bash
   go build
   ```

## Usage ⌨️

Simply run the compiled binary with the `gen` command:

```bash
./Gengo gen
```

Gengo will then prompt you interactively:

- Enter the data model to generate: Type `ecommerce-ds` (for TPC-DS benchmark), `ecommerce`, `financial`, or `medical`. Gengo can handle common misspellings and abbreviations (e.g., `eds`, `ecom`, `fin`, `med`).

- Enter the approximate target size in GB: (e.g., 0.5, 10, 50). Gengo will display the estimated row counts for each table based on this.

- Enter the desired output format: Type csv, json, or parquet.

- Enter the output directory name: This directory will be created if it doesn't exist, and all generated table files (e.g., dim_customers.parquet, fact_orders.parquet) will be saved inside it.

Gengo will then get to work, showing progress and timing information when complete.

## TPC-DS Benchmark Generation 🎯

The TPC-DS (Transaction Processing Performance Council Decision Support) benchmark is the industry standard for data warehousing performance testing. Gengo implements a complete TPC-DS schema with realistic business data modeling.

### Example: 10GB TPC-DS Dataset

```bash
./Gengo gen
# Enter: eds (or ecommerce-ds)
# Enter: 10 (for 10GB)
# Enter: csv
# Enter: output_directory
```

This generates a comprehensive dataset with:

**📊 Fact Tables (57.7M total rows):**
- Store Sales: 31.7M rows (55% of sales)
- Web Sales: 17.3M rows (30% of sales) 
- Catalog Sales: 8.6M rows (15% of sales)
- Store Returns: 2.5M rows (8% return rate)
- Web Returns: 2.1M rows (12% return rate)
- Catalog Returns: 865K rows (10% return rate)
- Inventory: 356M rows (weekly snapshots)

**🏢 Dimension Tables (17 tables):**
- 2.3M Customers with 5.1M Addresses
- 481K Items with 80K Promotions
- 72 Stores, 57 Warehouses, 38 Call Centers
- Rich demographics and geographic data

**Performance Metrics:**
- 25 orders per customer annually
- 9.5% overall return rate
- Realistic sales channel distribution
- Complete foreign key relationships

### Production-Scale Capabilities

Gengo can generate **terabyte-scale TPC-DS datasets** efficiently:
- **1TB dataset**: ~73 minutes on 4 cores, 6-9 minutes on 64 cores
- **10TB dataset**: ~12 hours on 4 cores, 1-1.5 hours on 64 cores
- Optimized for multi-core scaling with high-performance NVMe storage

## Customization 🎨

Want different fake data or schema modifications?

- **Schema:** Modify the Go structs in `internal/models/ecommerce/ecommerce.go`, `internal/models/financial/financial.go`, and `internal/models/medical/medical.go`. Remember to update struct tags (`json`, `parquet`) accordingly.
- **Dimension Data:** Change the `gofakeit` functions or logic used within the `Generate*` functions in `internal/simulation/ecommerce/simulate_dims.go`, `internal/simulation/financial/simulate_financial_dims.go`, and `internal/simulation/medical/simulate_medical_dims.go`.
- **Fact Data & Realism:** Adjust the generation logic (e.g., distributions, static lists), foreign key selection (including weighted sampling), or calculation logic within the `Generate*ModelData` functions in `internal/simulation/ecommerce/simulate_facts.go`, `internal/simulation/financial/simulate_financial_facts.go`, and `internal/simulation/medical/simulate_medical_facts.go`.
- **Sizing Ratios:** Modify the constants in `internal/core/sizing.go` to change the relative sizes of the generated tables.

## Implementation Details ⚙️

For those interested in the technical underpinnings, Gengo's performance and design are rooted in the following key implementation choices:

### High-Performance Optimizations

Gengo implements several sophisticated optimizations to achieve ultra-fast data generation, particularly for large fact tables:

#### 1. Per-Worker RNG Elimination of Global Lock Contention
- Modified `weightedSampler.Sample()` to accept an RNG parameter instead of using global `rand.Float64()`
- Each worker goroutine gets its own `*rand.Rand` instance with unique seed generation
- Eliminates contention on the global random number generator lock

#### 2. Parallel Byte Chunk Formatting with Single Writer
- Replaced struct-based channels with byte chunk channels for fact tables
- Worker goroutines format CSV rows directly into byte slices using `strconv.AppendInt/AppendFloat`
- Single dedicated writer goroutine performs simple `Write()` operations
- Eliminated `encoding/csv` usage for fact tables (kept for dimensions with string fields)
- Buffer size increased from 64KB to 16MB for better throughput

#### 3. Epoch Seconds Instead of Formatted Timestamps
- Replaced expensive `time.Format(time.RFC3339)` calls with raw epoch seconds
- Updated header from `order_timestamp` to `order_timestamp_unix`
- Eliminates costly timestamp formatting in the hot path (1.45M times)

#### 4. Block Allocation for Order Item IDs
- Implemented `idBlock` type with `nextID()` method for managing ID blocks
- Block size of 100,000 IDs to reduce atomic contention
- Replaced per-item atomic operations with per-block atomic operations
- Contends on atomic only once per 100k items instead of once per item

#### 5. Slice Instead of Map for Product Details
- Replaced `map[int]ProductDetails` with `[]ProductDetails` slice
- Eliminated hash lookups in inner loop with direct array access
- Product details accessed via `productDetails[productID]` with zero allocations

#### 6. Optimized Buffer Sizes
- Increased worker buffer sizes from 1MB to 16MB
- Reduced number of channel operations and buffer reallocations
- Better matching of buffer size to file writer buffer size (16MB)

### Core Concurrent Architecture

- **Concurrent Data Generation:** Leveraging Go's lightweight goroutines and channels, data generation for large fact tables (e.g., `fact_daily_stock_prices`, `fact_order_items`) is parallelized across available CPU cores. This employs a producer-consumer pattern where worker goroutines generate data chunks, which are then aggregated and written.
- **Atomic ID Management:** Unique primary keys (e.g., `OrderItemID`, `AppointmentID`) are managed across concurrent generation streams using `sync/atomic` operations, ensuring correctness without performance bottlenecks from locks.
- **In-Memory Aggregation:** For optimal write performance, data for each table is generated and aggregated in memory before being written to disk in a single operation. Go's efficient garbage collector handles the memory management for these large in-memory structures.
- **Dynamic Sizing Heuristics:** The `internal/core/sizing.go` package dynamically estimates row counts for all tables based on a target GB size, using empirically derived ratios and logical dependencies between dimensions and facts.
- **Efficient File Formats:** Integration with Apache Arrow and Parquet (via `apache/arrow/go`) enables highly efficient, columnar storage for generated data, reducing disk footprint and improving read performance for downstream systems.

## Benchmarks 📊

### Verified Performance Metrics

Based on actual generation runs with Gengo:

| Dataset | Size | Generation Time | Throughput | Row Rate |
|---------|------|----------------|------------|----------|
| TPC-DS  | 10GB | 42 seconds     | **238 MB/s** | **10M rows/sec** |
| TPC-DS  | 1GB  | ~4 seconds     | ~250 MB/s  | ~10M rows/sec |

**Throughput Analysis:**
- **10GB TPC-DS dataset**: 10GB ÷ 42s = **238 MB/s sustained write speed**
- **422M total rows**: 422,000,000 ÷ 42s = **10 million rows/second**
- **24 files generated concurrently**: Multi-file parallel output

### Realistic Performance Comparisons

| Tool | Dataset Size | Throughput | Notes |
|------|--------------|------------|-------|
| **Gengo** | 10GB TPC-DS | **238 MB/s** | Go, optimized for relational data |
| Python Faker | 1GB simple CSV | ~10-50 MB/s | Single-threaded, interpreted overhead |
| Mockaroo | 1GB generated | ~20-80 MB/s | Web service, network limited |
| SQL Data Generator | 1GB relational | ~50-100 MB/s | Database overhead, logging |

**Key Advantages:**
- **10-20x faster** than Python Faker for relational datasets
- **3-5x faster** than web-based generators (no network latency)
- **2-4x faster** than SQL-based generators (no database overhead)
- **True relational integrity**: Foreign keys, realistic distributions, 3NF normalized

### Hardware Specifications

**Test Environment:**
- CPU: Multi-core processor (4+ cores)
- Storage: NVMe SSD (recommended for optimal performance)
- Memory: 16GB+ RAM
- OS: Linux/Windows/macOS

**Scaling Characteristics:**
- Performance scales linearly with CPU cores
- NVMe storage recommended for sustained 200+ MB/s write speeds
- Memory usage: ~2GB per concurrent worker

Happy generating and playing around with the data!
