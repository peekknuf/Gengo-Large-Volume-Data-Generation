## E-commerce CSV Data Generation: End-to-End Documentation

### Overview
This document explains exactly how the e-commerce CSV dataset is generated in this repository, from the CLI entrypoint through data simulation and CSV writing. It describes inputs, parameters, data schemas, processing steps, concurrency, output files, and dependencies.

High-level flow:
- CLI: select model = ecommerce, target size (GB), output format, output directory
- Orchestrator: spawn concurrent generation/writing of dimension tables and stream-writing of fact tables
- Simulation: synthesize realistic dimension entities and high-volume orders/items
- CSV Writers: write dimensions via slice writers and facts via chunked streaming writers

Key modules involved:
- CLI entrypoint: main.go
- Orchestration: internal/core/orchestrator.go
- User input + sizing: internal/core/input.go, internal/core/sizing.go
- E-commerce models (schemas): internal/models/ecommerce/ecommerce.go
- E-commerce data simulation: internal/simulation/ecommerce/
  - simulate_dims.go (dimensions)
  - simulate_facts.go (facts)
- CSV writers: internal/formats/
  - csv.go (slice writers and stream writers)
  - csv_chunks.go (chunked writer used for facts)


### Entry Points and Configuration
- Command: ./Gengo gen
- The root command and generate subcommand are defined in main.go. The gen command:
  1) Prompts the user via core.GetUserInputForModel
  2) Seeds randomness
  3) Calls core.GenerateModelData(modelType, counts, format, outputDir)

Key prompt parameters (input.go):
- modelType: ecommerce | financial | medical (with fuzzy matching)
- targetGB: approximate target output size in GB (float)
- format: csv | json | parquet
- outputDir: directory to create and write into

Sizing for ecommerce (sizing.go):
- Given targetGB, CalculateECommerceRowCounts produces counts for:
  - Customers, CustomerAddresses, Suppliers, Products, ProductCategories, OrderHeaders, OrderItems
- Ratios/constants used:
  - AvgItemsPerOrder = 5.0
  - DefaultOrdersPerCustomerRatio = 10.0
  - AvgAddressesPerCustomer = 1.5
  - DefaultProductsPerSupplierRatio = 25.0
  - Effective size per order item (calibrated): 148 bytes
- Suppliers derived from customers (~1 per 100 customers); products derived from suppliers.

Important nuance: For ecommerce, the orchestrator writes CSV regardless of the selected format string. The format argument is not used for ecommerce writing; CSV is always produced.


### Orchestration: E-commerce Pipeline
Function: core.GenerateModelData → generateECommerceDataConcurrently (orchestrator.go)

Steps orchestrated for ecommerce:
1) Ensure outputDir exists
2) Create channels to stream fact data as byte chunks:
   - headersChunkChan chan []byte → fact_orders_header.csv
   - itemsChunkChan chan []byte → fact_order_items.csv
3) Start two writer goroutines immediately using formats.WriteCSVChunks (chunked CSV writer) with static headers
4) Generate dimensions concurrently:
   - customers + customerAddresses
   - suppliers
   - productCategories
   - products (waits for suppliers and categories to complete)
5) Write dimensions concurrently to CSV via dedicated slice writers
6) Once customers and products are ready, start fact generation concurrently:
   - Build supporting structures (customerIDs, productDetails slice, productIDsForSampling)
   - Call ecommerce.GenerateECommerceModelData(..., headersChunkChan, itemsChunkChan)
7) Wait for all writers to finish; surface any error

File naming convention (in outputDir):
- dim_customers.csv
- dim_customer_addresses.csv
- dim_suppliers.csv
- dim_product_categories.csv
- dim_products.csv
- fact_orders_header.csv
- fact_order_items.csv


### Data Models (Schemas)
Types are defined in internal/models/ecommerce/ecommerce.go. CSV columns and types are enforced by writer functions.

Dimension tables
- Customers (dim_customers.csv)
  - customer_id:int, first_name:string, last_name:string, email:string
- CustomerAddresses (dim_customer_addresses.csv)
  - address_id:int, customer_id:int, address_type:string, address:string, city:string, state:string, zip:string, country:string
- Suppliers (dim_suppliers.csv)
  - supplier_id:int, supplier_name:string, country:string
- ProductCategories (dim_product_categories.csv)
  - category_id:int, category_name:string
- Products (dim_products.csv)
  - product_id:int, supplier_id:int, product_name:string, category_id:int, base_price:float(2)

Fact tables
- Order Headers (fact_orders_header.csv)
  - order_id:int, customer_id:int, shipping_address_id:int, billing_address_id:int, order_timestamp_unix:int64, order_status:string
- Order Items (fact_order_items.csv)
  - order_item_id:int64, order_id:int, product_id:int, quantity:int, unit_price:float(2), discount:float(4), total_price:float(4)


### Generation Logic (Simulation)
Dimensions (simulate_dims.go)
- Customers
  - First/last names via gofakeit; email provider randomly chosen from a small list; email formed as firstname.lastnameN@provider, then numerified
  - customer_id is sequential starting at 1
- CustomerAddresses
  - For each customer, 1–3 addresses
  - address_type randomly chosen from [shipping, billing]
  - address fields via gofakeit Address(); country hard-coded to "United States"
  - address_id sequential starting at 1
- Suppliers
  - supplier_name via gofakeit.Company(); country via gofakeit.Country()
  - supplier_id sequential starting at 1
- ProductCategories
  - Ten fixed categories; category_id sequential 1..10
- Products
  - product_name via gofakeit.ProductName()
  - Base price sampled from Normal(mu=75, sigma=45), clamped to at least ~5 by resampling into [5, 25] when computed < 5
  - supplier_id and category_id sampled uniformly from provided lists
  - product_id sequential starting at 1

Facts (simulate_facts.go)
- Concurrency
  - runtime.NumCPU workers; each generates a contiguous block of order IDs
  - Per-worker RNG seeds (baseSeed + golden-ratio constant per worker) to avoid contention
- Customer and product sampling
  - Weighted sampler prefers lower index IDs proportionally to 1/sqrt(rank)
- Addresses
  - Build a map customer_id → []address_id in two passes (count, then pre-allocate and fill), then sample randomly per order
- Order generation
  - Each order: 1–10 items
  - order_status randomly from [Pending, Processing, Shipped, Delivered, Cancelled, Returned]
  - order_timestamp_unix uniformly over the last ~5 years (epoch seconds)
- Item generation
  - quantity in [1, 15]
  - unit_price = product.BasePrice
  - discount: 30% chance; if applied, drawn from [0.05, 0.25]
  - total_price = quantity * unit_price * (1 - discount)
- Order item IDs
  - Global int64 counter allocated in blocks of 100,000 to minimize atomic contention
- Output buffering
  - Build CSV rows into 16MB byte buffers per worker and flush into channels when buffers reach capacity or when worker completes


### CSV Writing and Output Structure
Dimensions (csv.go)
- Dedicated writer per dimension uses encoding/csv. It writes headers then all records at once via csv.Writer.WriteAll
- Success message: "Successfully wrote N records to <path>"

Facts (csv_chunks.go)
- The orchestrator uses formats.WriteCSVChunks with static header strings
- Writer opens file, writes header + newline, and then reads byte slices from a channel and appends to the file using a 16MB buffered writer
- Record count derived by counting newlines in the received chunks; success message printed


### Code Flow and Key Calls (selected snippets)
- Entrypoint and orchestration
- Writers used for dimensions and facts
- Fact generation invocation

Example: start chunk writers for facts and write one of the dimensions

```mermaid
flowchart TD
  A[./Gengo gen] --> B(core.GetUserInputForModel)
  B --> C{modelType==ecommerce}
  C --> D[generateECommerceDataConcurrently]
  D --> E1[spawn WriteCSVChunks for fact_orders_header.csv]
  D --> E2[spawn WriteCSVChunks for fact_order_items.csv]
  D --> F1[GenerateCustomers + GenerateCustomerAddresses]
  D --> F2[GenerateSuppliers]
  D --> F3[GenerateProductCategories]
  D --> F4[GenerateProducts (after F2,F3)]
  F1 --> G1[WriteCustomersToCSV]
  F1 --> G2[WriteCustomerAddressesToCSV]
  F2 --> G3[WriteSuppliersToCSV]
  F3 --> G4[WriteProductCategoriesToCSV]
  F4 --> G5[WriteProductsToCSV]
  F1 & F4 --> H[GenerateECommerceModelData → stream chunks]
  H --> E1
  H --> E2
```


### File Paths and Naming Conventions
- All files are written directly under the user-provided outputDir
- Prefixes: dim_ for dimensions, fact_ for transactional fact tables
- Fixed filenames (CSV only for ecommerce, despite "format" selection):
  - dim_customers.csv, dim_customer_addresses.csv, dim_suppliers.csv, dim_product_categories.csv, dim_products.csv
  - fact_orders_header.csv, fact_order_items.csv


### Dependencies and Requirements
- Go standard library: encoding/csv, bufio, os, sync, time, math/rand, etc.
- CLI: github.com/spf13/cobra
- Fake data: github.com/brianvoe/gofakeit/v6
- Distributions: gonum.org/v1/gonum/stat/distuv
- For non-ecommerce formats: Apache Arrow for Parquet (not used by ecommerce path as currently implemented)

Build and run
- go build
- ./Gengo gen


### Business Logic and Validation Rules
- User input validation: supported model/format; positive GB; non-empty output directory
- Sizing rules produce consistent ratios across entities
- Fact generation preconditions:
  - If numOrders <= 0: closes channels and returns
  - If any of customerIDs, productIDsForSampling, or customerAddresses are empty: closes channels and returns error
- CSV writing:
  - Dimension writers write headers and all records; facts write headers then stream rows
  - Chunk writers count records by newline characters; flush is managed by buffered writer


### Notes and Nuances
- Timestamp format: For fact_orders_header.csv, order_timestamp_unix is an epoch seconds integer (not RFC3339). This is set by the orchestrator’s header and the simulation’s AppendInt calls.
- Format selection: Even if the user selects json or parquet, the ecommerce pipeline writes CSV outputs. (Financial/medical paths do honor the format via formats.WriteSliceData.)
- Concurrency: Dimension generation/writing and fact streaming are overlapped to maximize throughput.


### Pointers to Key Implementations
- Orchestrator: internal/core/orchestrator.go (generateECommerceDataConcurrently)
- Dimension simulation: internal/simulation/ecommerce/simulate_dims.go
- Fact simulation: internal/simulation/ecommerce/simulate_facts.go
- CSV writers (dimensions): internal/formats/csv.go
- Chunked writer (facts): internal/formats/csv_chunks.go
- Models (schemas): internal/models/ecommerce/ecommerce.go

