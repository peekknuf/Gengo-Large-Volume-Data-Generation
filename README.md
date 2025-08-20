# Gengo 🚀 - Relational Data Model Generator

Gengo is a command-line tool written in Go for rapidly generating large, synthetic **relational datasets** (dimension and fact tables). Need tons of structured fake data for testing data warehouses, BI tools, demos, or benchmarks without waiting forever? Gengo's got your back!

It was originally built because generating millions of rows using scripting languages can often be slow. Go's performance helps speed things up significantly, even when generating related tables.

## Features ✨

- **Ultra-Fast:** Leverages Go's performance and sophisticated optimizations for **ultra-fast** data generation, achieving **up to 17x speed improvements** over previous versions for relational models.
- **Relational Model:** Generates predefined 3NF data models for:
    - **E-commerce:** `dim_customers`, `dim_customer_addresses`, `dim_suppliers`, `dim_products`, `fact_orders_header`, `fact_order_items`
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

- Enter the data model to generate: Type `ecommerce`, `financial`, or `medical`. Gengo can handle common misspellings and abbreviations (e.g., `ecom`, `fin`, `med`).

- Enter the approximate target size in GB: (e.g., 0.5, 10, 50). Gengo will display the estimated row counts for each table based on this.

- Enter the desired output format: Type csv, json, or parquet.

- Enter the output directory name: This directory will be created if it doesn't exist, and all generated table files (e.g., dim_customers.parquet, fact_orders.parquet) will be saved inside it.

Gengo will then get to work, showing progress and timing information when complete.

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

Gengo has been significantly optimized for speed, especially for generating complex relational datasets. The benchmarks below reflect the performance after implementing concurrent data generation strategies and additional high-performance optimizations.

| Data Model          | Size | Format | Initial Time | Previous Time | Final Time | Improvement |
| ------------------- | ---- | ------ | ------------ | ------------- | ---------- | ----------- |
| E-commerce          | 1GB  | CSV    | 23s          | 51s           | **3s**     | ~17x        |
| Financial           | 2GB  | CSV    | 1m 50s       | 35s           | **8s**     | ~23x        |

These improvements were achieved through:
1. Parallel byte chunk formatting with single writer
2. Per-worker RNG elimination of global lock contention
3. Epoch seconds instead of formatted timestamps
4. Block allocation for order item IDs
5. Slice instead of map for product details
6. Optimized buffer sizes (16MB vs 64KB)

_(Note: Actual performance will vary based on your hardware.)_

Happy generating and playing around with the data!
