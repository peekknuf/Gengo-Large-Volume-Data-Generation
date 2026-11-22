# Load CSV datasets into PostgreSQL

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

## Configure and check connections

Gengo uses the installed `psql` client for checks and imports. No Go database
driver is required for this execution path. Copy `.env.example` to a local `.env`
and replace its endpoint placeholders. Keep credentials outside version control.
`PGPASSWORD` holds the database password separately from the URLs, so special
characters do not need URL encoding. Passwords embedded in URLs must be
percent-encoded instead.

The environment file is loaded only when you pass `--env-file`. Existing process
variables take precedence over file entries. A password embedded in the selected
URL takes precedence over `PGPASSWORD`, matching libpq connection-string behavior.
Gengo removes URL passwords from process arguments and redacts configured URLs
and passwords from captured client output. Password files and other normal libpq
credential sources remain supported.

| Target | Connection variable | Supabase endpoint |
| --- | --- | --- |
| `direct` | `GENGO_DIRECT_DATABASE_URL` | Project host, port 5432. |
| `shared` | `GENGO_TRANSACTION_DATABASE_URL` | Shared Supavisor transaction pooler, port 6543. |
| `dedicated` | `GENGO_DEDICATED_DATABASE_URL` | Project host, port 6543, dedicated PgBouncer transaction pooler. |
| `libpq` | Normal libpq settings | `PGHOST`, `PGPORT`, `PGUSER`, `PGDATABASE`, service files, and credential sources. |

```sh
gengo db check --env-file .env --target direct
gengo db check --env-file .env --target shared
gengo db check --env-file .env --target dedicated
```

Each check runs a read-only query for database name, role, server version, and a
literal connection-check value. It does not measure capacity or prove that a
connection bypasses a pooler. Target names identify the configured connection
string, not independently detected server topology.

Use TLS for remote endpoints. The example URLs set `sslmode=require`; use
`verify-full` with the appropriate trusted certificate for server identity
verification. Supabase's direct and dedicated endpoints require IPv6 unless the
project has the IPv4 add-on. The shared pooler uses IPv4. Copy the exact host and
username from the project's Connect panel.

The dedicated pooler requires a qualifying Supabase plan and enabled service.
Both poolers use transaction mode. Shared Supavisor transaction mode does not
support prepared statements. Dedicated PgBouncer supports them on supported
versions, but workload protocol comparisons are not implemented yet. See
[Supabase connection documentation](https://supabase.com/docs/guides/database/connecting-to-postgres).

## Execute an import

```sh
gengo load run --dataset data/ecommerce --schema gengo_experiment \
  --output data/ecommerce-import-run --env-file .env --target direct
```

This command validates the dataset, retains a new import plan, then runs its
`run.psql` through `psql -X -w` with `ON_ERROR_STOP` enabled. It creates a new schema,
loads data, checks counts, builds primary keys, validates foreign keys, and
collects statistics. It never drops or replaces an existing schema. Use a private
schema, not one exposed through Supabase's Data API.

Imports accept `direct` or `libpq` targets. For `libpq`, configure an unpooled or
session connection appropriate for administrative operations. Transaction-pooler
targets are reserved for connection checks and future workload execution.

Checks default to a 30-second timeout; imports default to 10 minutes. Override
these with `--timeout 15s` or `--timeout 20m`. Ctrl+C and execution timeouts stop the
client. Active transactions roll back when the connection closes; previously
committed import stages remain. The timeout covers client execution, not dataset
validation and script preparation. Client output is buffered and redacted before
Gengo prints it, so stage messages appear when the client exits.

If an import fails, inspect the retained scripts and completed database stages.
A full retry requires a new schema and a new script output directory. Gengo does
not automatically delete a partially imported schema or resume failed stages.

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

## Verify the live import

Live integration tests require explicit opt-in and an existing direct endpoint.
They create uniquely named `gengo_test_*` and `gengo_cli_*` schemas and drop only
those test schemas during cleanup. Run them only against a database where that
temporary data and DDL are permitted. Interrupted tests can leave their isolated
schemas behind.

```powershell
$env:GENGO_POSTGRES_TEST_ENV_FILE = (Resolve-Path .env).Path
go test ./internal/core -run '^TestLivePostgresCSVLoad$' -count=1
go test ./tests -run 'TestGenerationControls/load_run_live$' -count=1
Remove-Item Env:GENGO_POSTGRES_TEST_ENV_FILE
```

The core test checks all declared row counts, seven primary keys, eight validated
foreign keys, and a join across items, orders, customers, and products. It also
checks existing-schema rejection, a CSV path containing an apostrophe, and
data-load rollback after a deliberate count mismatch. The CLI test exercises
generation, connection checking, import execution, and existing-schema rejection.
Without opt-in, these tests skip database access.

The first live verification on 22 November 2026 passed against Supabase
PostgreSQL 17.12. Direct, shared transaction, and dedicated transaction endpoints
each passed the connection check. Workload execution remains pending in
[the product direction](product-direction.md).
