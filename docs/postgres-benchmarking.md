# Run PostgreSQL workload benchmarks

For application queries, see [custom SQL workloads](custom-sql.md). This guide
uses the built-in customer lookup to explain shared controls and measurements.

Build Gengo and install PostgreSQL client tools with `psql` and `pgbench` 17 or
later on `PATH`. Configure the endpoint variables described in
[PostgreSQL loading](postgres-loading.md). The workload is read-only, but running
it still consumes database connections and compute. Start with small concurrency
levels on a database where test traffic is permitted.

## Prepare the dataset and scenario

Generate and import an e-commerce CSV dataset into a new private schema:

```sh
gengo gen --model ecommerce --size 0.001 --format csv --seed 42 \
  --as-of 2026-11-22 --output data/ecommerce
gengo load run --dataset data/ecommerce --schema gengo \
  --output data/ecommerce-import --env-file .env
```

Edit `examples/postgres/lookup.yaml` to match the dataset directory and imported
schema. Dataset paths are relative to the scenario file, not the working
directory. No connection URLs or passwords belong in the scenario. The configured
target names select environment-based connections.

```yaml
version: 1
name: ecommerce-customer-lookup
dataset: ../../data/ecommerce
schema: gengo
targets: [direct, shared, dedicated]
workload: customer-lookup
seed: 42
protocol: simple
concurrency: [1, 2, 4]
warmup: 2s
duration: 10s
thresholds:
  max_p95_ms: 250
  max_error_rate: 0
  min_tps: 1
```

Each iteration chooses a uniform customer ID from `1..customer_count` and runs
one point lookup. The built-in workload version is 1. The workload seed is
separate from the dataset generation seed; both are recorded in results. Seed
zero is valid. A seed does not make timing or concurrent scheduling reproducible.

Version 1 accepts `customer-lookup` and `custom-sql` with the simple SQL protocol,
persistent client connections and one pgbench worker thread. For customer lookups,
dataset metadata must describe a completed e-commerce dataset with
a matching dataset identity. Before each target, Gengo checks that database
customer keys are unique and cover the manifest's contiguous `1..N` range. This
checks customer suitability, not the identity of every imported table.

## Run the sweep

```sh
gengo bench run --scenario examples/postgres/lookup.yaml \
  --env-file .env --output data/lookup-run-001
```

The output directory must not exist. Gengo never replaces saved experiments.
It runs targets in their listed order, then each target's increasing concurrency
levels. Each level has a separate warm-up process followed by a measurement
process. Warm-up does not contribute measurement samples. Those processes use
different connections; warm-up prepares database and pooler state, not the exact
connections used for measurement. No data reset is needed for this read-only
workload.

The load model is closed-loop concurrency: each client starts another iteration
after finishing its previous one. Client count is not a backend connection count
or a simultaneous executing-query count. Rate-driven traffic and connection
churn are not supported yet.

`warmup` and `duration` must be explicit whole-second durations. Warm-up may be
`0s`; measurement must be at least `1s`. Each is limited to five minutes. Up to 32
strictly increasing concurrency levels are accepted, with 1 to 1024 clients per
level. These are input bounds, not recommendations for a hosted database.

pgbench's duration can include initial client connection setup. Slow remote
connections can leave little or no time for queries in a short phase. Results
record initial connection time separately, client-process wall time, and the
active pgbench interval inferred from completed iterations divided by reported
TPS. This interval is subject to pgbench's printed TPS precision. A measurement
with no completed iterations is an error, not a successful zero-latency run.
Increase duration if startup consumes the test window.

Ctrl+C stops the active client and saves cancellation status when Gengo can still
write the result. Version and dataset checks have a 30-second timeout. Each load
phase has its configured duration plus a 30-second execution allowance. A killed
process or power failure can leave a result marked `running`; it must not be
treated as completed.

## Read the results

The terminal summary reports status, TPS, successful-iteration p95, failure
counts, and the highest passing tested concurrency per target. A threshold breach
marks that step failed, but subsequent configured levels and targets still run.
Execution and preflight failures also mark the experiment failed. A failure gives
the command a nonzero exit code after saving results. Cancellation ends the sweep.

The output directory contains:

- `result.json`, a version-1 result updated after each completed phase and step.
- `workload.sql`, the exact executed built-in or wrapped custom script.
- Per-phase directories with redacted `pgbench.txt` client output.
- Per-measurement `transactions.*` logs with numeric per-iteration timings.

`result.json` records resolved scenario settings, dataset identity and generation
seed, workload version and seed, PostgreSQL and pgbench versions, target labels,
phase statuses, timings, thresholds, and capacity. It does not store the dotenv
file or connection URLs. Hostnames may appear in redacted connection diagnostics.

TPS comes from pgbench and excludes its initial connection time. Mean and
nearest-rank p50/p95/p99 come from all successfully completed iteration log rows,
in milliseconds. Gengo checks that success and failure log counts match the
pgbench summary. Failed iterations do not contribute latency percentiles. The
error rate is failed divided by completed plus failed iterations. Transaction
retries are disabled.

Thresholds pass when p95 is at most `max_p95_ms`, error rate is at most
`max_error_rate`, and TPS is at least `min_tps`. Capacity is the highest tested
client count passing all thresholds, not an estimate beyond tested levels. Zero
means no measured level passed. A target preflight error has no capacity entry.
These numbers do not establish saturation when the sweep never reaches a failing
level.

Requested iteration counts are unknown for duration-driven load and serialize as
`null`. Separate timeout counts are unavailable and also serialize as `null`.
Skipped iterations are zero because this runner does not schedule arrival-rate
traffic. Client-observed iteration latency includes script processing and network
effects. It is not isolated server SQL latency. Server query time, pooler waiting,
client queuing, backend telemetry, and dataset creation/import timings are listed
as unavailable rather than fabricated.

Log analysis rejects phases exceeding 64 MiB of transaction logs or one million
successful iterations. Raw logs remain for inspection. These analysis limits do
not bound disk use during execution. Use short scenarios until streaming budgets
and log-management controls are implemented.

## Verification and remaining work

Unit tests cover strict YAML validation, explicit seed zero, dataset identity,
summary/log agreement, exact percentile fixtures, threshold boundaries, analysis
limits, cancellation persistence, and output reuse rejection. The opt-in live CLI
test also runs passing and deliberately failing two-level sweeps, checks saved
measurements, and verifies continuation after threshold failures:

```powershell
$env:GENGO_POSTGRES_TEST_ENV_FILE = (Resolve-Path .env).Path
go test ./tests -run '^TestGenerationControls/load_run_live$' -count=1
Remove-Item Env:GENGO_POSTGRES_TEST_ENV_FILE
```

On 22 November 2026, a ten-second, one/two-client smoke sweep passed all six load
steps across direct, shared transaction, and dedicated transaction endpoints on
Supabase PostgreSQL 17.12 with pgbench 19. Intermittent DNS failures affected earlier
attempts. Verification used temporary libpq `hostaddr` URL parameters with resolved
addresses while retaining the original hostnames and TLS. These were process-only
overrides, not changes to the stored environment file. Do not permanently pin a
hosted endpoint's current IP; it can change.
The opt-in CLI integration test also passed its successful and deliberately
failing sweeps with a temporary direct-endpoint hostaddr override.

Short live runs prove execution and result handling, not endpoint performance.
Run comparisons without overlapping experiments or unrelated database load.
Saved experiments can be compared offline with
`gengo bench compare BASELINE_JSON CANDIDATE_JSON`; see
[benchmark comparisons](benchmark-comparisons.md) for compatibility checks and
interpretation limits.

Custom [read/write experiments](write-workloads.md) now support setup, reset,
and declared state reuse. Longer repeated measurements, varying endpoint order,
built-in write presets, arrival rates, backend telemetry, additional
protocols, and a local PostgreSQL/PgBouncer reference environment remain roadmap
work.
