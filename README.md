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
  foreign-key consistency is incomplete. Do not use it for audited TPC-DS results.
- Some generators keep all rows in memory. Large runs can require substantial RAM.
- Use an empty or new output directory. Gengo publishes it after generation succeeds.

## Check

```sh
go test ./...
go vet ./...
```

