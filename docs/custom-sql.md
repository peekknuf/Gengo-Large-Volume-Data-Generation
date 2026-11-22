# Run custom SQL workloads

Set `workload: custom-sql` and `sql_file: query.sql` in a version-1 benchmark
scenario. SQL paths resolve relative to the YAML file, regardless of the working
directory. Start with `examples/postgres/custom.yaml` and replace its SQL with
queries against your application tables:

```sh
gengo bench run --scenario examples/postgres/custom.yaml \
  --env-file .env --output data/custom-run-001
```

The existing [benchmark controls and measurements](postgres-benchmarking.md)
apply, including concurrency levels, warm-up, duration, thresholds, cancellation,
and saved results. Scripts use pgbench syntax, including seeded variables such
as `\set id random(1, 1000)` and multiple SQL statements. One iteration executes
the entire script.

## Read-only transaction contract

Gengo wraps each iteration in `BEGIN READ ONLY` and `COMMIT`. Reported latency
includes that transaction and all statements. Ordinary persistent writes fail.
Do not include transaction control, change read-only settings, or use pgbench
shell commands. Scripts are trusted code; the wrapper is not a sandbox for
untrusted SQL. Use a database role limited to reads when restrictions must hold
independently of the script. Write workloads and setup/reset scripts require the
next state-policy increment.

Custom SQL requires neither `dataset` nor `schema`. Qualify table names, such as
`public.orders`, in the SQL. The `schema` field does not set a search path or
substitute identifiers. If supplied, `dataset` must satisfy the existing completed
e-commerce manifest checks. It provides the pgbench variable `:customer_count`
and records dataset identity. The custom preflight checks connectivity and server
version only; it does not validate customer ranges, application tables, or
application data identity. Without a dataset, operators must verify equivalent
starting data themselves.

## Saved workload identity

SQL files must be nonempty regular files of at most 1 MiB without NUL bytes.
Gengo snapshots the file when loading the scenario. Later edits cannot alter
that run. The output directory retains the exact wrapped script as `workload.sql`
and its SHA-256 as `workload_sha256` in `result.json`. Keep secrets out of SQL,
since the script is retained verbatim.

Use `gengo bench compare BASELINE_JSON CANDIDATE_JSON` for offline comparisons.
Workload kind and script hash must match, along with the existing compatibility
settings. Changing SQL, comments, or whitespace changes the hash and rejects the
comparison. Moving identical SQL to another path is allowed. Original source
files need not exist when comparing. Runs without dataset metadata can only
compare with other runs without it. Hashes detect workload differences; they do
not authenticate result files.

SQL errors preserve diagnostics and mark the phase as errored. Subsequent load
levels still run. These results cannot be compared as completed measurements.

## Verify against PostgreSQL

The opt-in CLI test runs three custom sweeps, verifies transaction read-only
status, warm-up, measured iterations, saved hashes and tested capacity, compares
identical SQL, and rejects changed SQL:

```powershell
$env:GENGO_POSTGRES_TEST_ENV_FILE = (Resolve-Path .env).Path
$env:GENGO_POSTGRES_TEST_TARGETS = 'direct, shared, dedicated'
go test ./tests -run '^TestCustomSQLLive$' -count=1 -v
go test ./tests -run '^TestCustomSQLFailuresLive$' -count=1 -v
```

The target list defaults to `direct`. Unit tests also cover invalid files,
optional dataset metadata, seeded pgbench variables, and source edits after
scenario loading.

On 22 November 2026, live CLI verification passed all 18 measured steps and
their warm-ups across direct, shared Supavisor, and dedicated PgBouncer targets.
Offline CLI comparisons accepted identical scripts and rejected a changed script.
Separate live checks passed table reads, rejected persistent writes with the
expected read-only transaction error, and preserved SQL errors while continuing
the sweep. Build, vet, and the full Windows test suite passed with
`GENGO_TEST_SIZE_GB=0.00001` for the eight generation cases. These short runs
verify functionality, not database capacity or endpoint performance.
