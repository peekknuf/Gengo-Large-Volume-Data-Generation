# Gengo

Generate synthetic relational datasets as CSV or Snappy-compressed Parquet.
Requires Go 1.25 or later.

## Build

```sh
git clone https://github.com/peekknuf/Gengo-Large-Volume-Data-Generation.git
cd Gengo-Large-Volume-Data-Generation
go build -o gengo .
```

## Run

```sh
./gengo gen --model ecommerce-ds --size 1 --format csv --output data/example
```

On Windows, build with `-o gengo.exe` and run `.\gengo.exe`.
Run `gengo gen` without flags for interactive prompts.
Use `--rows companies=3 --rows daily_stock_prices=17` to override counts from a financial size plan.
Row overrides use lowercase plan names with underscores. Add `--dry-run` to inspect the resolved counts.
Use `--seed 0 --as-of 2026-11-21 --rows-per-file 100000` for repeatable output.
Each completed run includes a manifest. See [reproducibility](docs/reproducibility.md)
for guarantees, file partitioning, and timestamp units.

| Model | Output |
| --- | --- |
| ecommerce | Customers, addresses, suppliers, categories, products, orders, order items |
| ecommerce-ds | 17 dimensions; store, catalog, and web sales |
| financial | Companies, exchanges, daily stock prices |
| medical | Patients, doctors, clinics, appointments |

## Limits

- Formats: CSV and Parquet. JSON is not supported by the complete pipeline.
- Size is approximate, in GiB. Compression and minimum table sizes affect output.
- The retail model is TPC-DS-inspired. Returns and inventory are not generated;
  zero in a site or call-center closing date denotes an open location.
- Retail date keys are Unix days. E-commerce order timestamps use Unix seconds
  in CSV and microsecond timestamps in Parquet.
- Financial prices reference an exchange per price row.
- Some generators keep all rows in memory. Large runs can require substantial RAM.
- Use an empty or new output directory. Gengo publishes it after generation succeeds.

## Check

```sh
go test ./...
go vet ./...
```
