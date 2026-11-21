# PostgreSQL CSV import plans

```sh
gengo gen --model ecommerce --size 0.001 --format csv --seed 0 \
  --as-of 2026-11-21 --output data/ecommerce
gengo load plan --dataset data/ecommerce --schema gengo \
  --output data/ecommerce-import
```

The planner reads the completed dataset manifest, checks file presence and sizes,
checks CSV headers, and generates psql scripts. The first implementation supports
the `ecommerce` model in CSV format. Script generation works without a database
connection. Live loading uses the PostgreSQL psql client.

Use an empty or new output directory for the scripts. The generated SQL creates
the requested database schema. An existing schema causes schema creation to fail.
Database connection details come from psql's normal libpq configuration, including
`PGHOST`, `PGPORT`, `PGUSER`, `PGDATABASE`, and supported password sources.

```sh
psql -X -f data/ecommerce-import/run.psql
```

Run psql on the machine containing the generated dataset. Import scripts contain
absolute client-side CSV paths, with Windows paths normalized to forward slashes.
Moving the dataset requires regenerating the scripts.

## Import stages

| File | Operation |
| --- | --- |
| `schema.sql` | Create the schema and seven tables. |
| `load.sql` | Copy every manifest-listed shard using client-side `\copy`, verify counts, and commit the data load. |
| `verify.sql` | Compare table row counts with the dataset manifest. Also runnable separately. |
| `indexes.sql` | Build the seven primary-key indexes. |
| `foreign_keys.sql` | Add eight foreign keys using `NOT VALID`. |
| `validate_foreign_keys.sql` | Validate those foreign keys against the loaded rows. |
| `analyze.sql` | Collect PostgreSQL planner statistics. |
| `run.psql` | Run the stages in order with statement timing and stop on errors. |

Schema creation, the data load, index creation, foreign-key creation, and
foreign-key validation each have their own transaction. A load error rolls back
that data-load transaction. Previously completed stages remain committed and
available for inspection. Each SQL stage can be invoked independently with psql
and `ON_ERROR_STOP` enabled.

CSV content is checked by PostgreSQL during the actual import. Planning checks
headers and manifest metadata. Actual row counts are verified inside the data-load
transaction, and relational validation follows during the foreign-key stage.

## Type and value mappings

Column order and names are derived from the current e-commerce model structs.
Integer keys use `integer`, 64-bit values use `bigint`, float64 values use
`double precision`, and strings use `text`. Columns are required because these
model structs represent required values.

The order-header CSV encoding stores `order_timestamp_unix` as Unix seconds.
Its PostgreSQL column is `bigint`; use `to_timestamp(order_timestamp_unix)` in
queries when a timestamp is needed. String columns use `FORCE_NOT_NULL` during
CSV import so empty Go strings retain their empty-string value.

## First workload example

`examples/postgres/customer-lookup.sql` contains a pgbench point-lookup script
using the default `gengo` schema. Supply `customer_count` from the dataset's
`identity.counts.dim_customers` manifest entry. The workload chooses customer IDs
uniformly in that range. A different destination schema requires adjusting the
SQL file.

Live PostgreSQL validation and integrated benchmark execution are tracked in
[the product direction](product-direction.md).
