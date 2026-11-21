# Reproducible generation

```sh
gengo gen --model medical --size 0.001 --format csv --output data/example \
  --seed 0 --as-of 2026-11-21 --rows-per-file 100000 --workers 4
```

The seed is an unsigned 64-bit integer. Zero is a valid seed. When omitted,
Gengo chooses a seed once and reports it. The default reference date is today
in UTC. `--as-of` accepts `YYYY-MM-DD`, from 1941-01-01 through 9999-12-31.

`--size` estimates row counts. Repeat `--rows table=count` to override individual
counts, using names such as `patients`, `appointments`, `companies`,
`daily_stock_prices`, `order_headers`, and `store_sales`. Parent dimensions must
be present for any generated children. `--dry-run` prints the resolved settings
and counts without generating output.

## Guarantee

With the same model, resolved counts, seed, reference date, file partitioning,
and generator/dependency versions, changing workers preserves logical rows and
file membership. CSV data files are byte-identical. Parquet preserves schemas
and values. Parquet byte identity across library versions and platforms is
outside the guarantee.

CSV and Parquet represent the same values for the same explicit row plan.
E-commerce CSV order timestamps use Unix seconds. Parquet stores those timestamps
in microseconds. Medical timestamps retain microsecond precision. Product prices
use cent precision in both formats. Decimal spelling can differ between formats.

Equal size targets can resolve to different row counts for different formats.
Use the same explicit counts when comparing their data.

`gengo-v1` derives local random streams from the seed, a stable table name, and
a partition or entity identifier. It uses FNV-1a and SplitMix64 seed mixing, Go
random sources, and the pinned gofakeit dependency. Changes to generation logic
require a new algorithm version. Faker instances belong to individual tables.

## File partitions and dates

`--rows-per-file` limits medical and retail fact rows per file. E-commerce
partitions contain that many orders, with up to ten items per order in the
corresponding item file. Financial generation groups whole companies where they
fit, splitting larger histories across files while continuing the price series.
Financial files contain at most the configured number of prices. Dimensions
remain in individual files. Zero-count tables can have no data file.

File names use stable numeric suffixes. They depend on row counts and file
partitioning. Workers control concurrency within each fact table. Changing
file partitioning can change generated values and the dataset identity.

Retail date keys cover the six calendar years before the reference year.
Orders and appointments cover the preceding five years through the reference
date. Financial series use consecutive calendar days ending on the reference
date. Histories extending before year 1 are rejected.

## Manifest and publication

Every completed run contains `manifest.json`. It records schema and algorithm
versions, the resolved seed, reference date, row counts, partition settings,
and the deterministic dataset identity. Workers and elapsed times are recorded
separately. The identity is shared by equivalent CSV and Parquet datasets.

Each file entry includes its table, actual written row count, byte size, and
first and last primary keys. Successful writers record counts after closing.
The manifest checks their totals against the row plan and checks physical file
sizes before publication. Full content and relationship validation runs in the
regression suite.

Generation time and manifest verification time are reported separately.
Completion includes flushing and closing files through the operating system.
Durability follows the operating system's normal buffered-write behavior.

Gengo writes to a sibling staging directory and publishes the destination after
all generation and manifest checks succeed. Destinations must be empty or new.
On a recoverable failure, staging output is removed. Abrupt process termination
can leave a directory whose name includes `-partial-`.
