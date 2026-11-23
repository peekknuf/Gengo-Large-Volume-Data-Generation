# Gengo agent guide

## Start here

Gengo generates reproducible relational datasets and is growing into a PostgreSQL
performance-testing tool, with broader Supabase coverage planned. Read
[product direction](docs/product-direction.md) for agreed scope and delivery status,
then [codebase map](docs/codebase-map.md) for active paths and known gaps.

Named quick/thorough experiment profiles now supply versioned defaults with
explicit overrides. Supabase Metrics API snapshots accompany experiments when
SUPABASE_API_KEY is configured. Custom SQL,
read/write state policies, arrival-rate sweeps, connection experiments,
repetitions, and rotating endpoint order work.
Extend the existing sweeps and saved comparisons.
Repetitions and available Supabase Metrics API collection support this work. The
local PostgreSQL/PgBouncer fixture is optional backlog work. Generation budgets
and Linux/race verification remain open and do not gate these experiments.

Go 1.25 or later is required. The module is github.com/peekknuf/Gengo.
Generation supports ecommerce, ecommerce-ds, financial, and medical in CSV and
Parquet. PostgreSQL import planning and execution support ecommerce CSV.

## Active entry points

| Task | Start with |
| --- | --- |
| Generation flags and command | main.go |
| Input, size planning, row overrides, prerequisites | internal/core/input.go, sizing.go, validation.go |
| Model dependencies, staging, publication | internal/core/orchestrator.go |
| Seeded streams, logical partitions, completed-writer counts | internal/generation |
| Manifest and dataset identity | internal/core/run.go |
| Domain row generation | internal/simulation/<model> |
| Model fields and serialization tags | internal/models/<model> |
| Generic and typed serialization | internal/formats and direct fact encoders in simulation |
| PostgreSQL import command | load_command.go |
| Connection checks, targets, dotenv, and psql execution | postgres_command.go, internal/postgres/client.go |
| Scenario YAML, pgbench sweeps, measurements, and results | benchmark_command.go, internal/benchmark |
| Offline saved-run comparisons and compatibility | internal/benchmark/compare.go, docs/benchmark-comparisons.md |
| Endpoint rounds, repetition statistics, and capacity pass rules | internal/benchmark/repetitions.go, docs/repeated-experiments.md |
| Named quick/thorough defaults and overrides | internal/benchmark/profiles.go, docs/experiment-profiles.md |
| Manifest checks and psql script planning | internal/core/load_plan.go |
| E-commerce column and relationship metadata | internal/schema/ecommerce.go |

The CLI calls internal/simulation. generation.Config owns the reproducibility
contract and generation.Partitions schedules fact work. The unused alternate
generator framework and its worker pool have been removed.

## Commands

~~~sh
go build -o bin/gengo .
go test ./...
go vet ./...
go test ./internal/core -run 'TestSmallDatasetContracts|TestReproducibleRuns'
go test ./internal/core -run TestCSVLoadPlan
go test ./tests -run TestGenerationControls
go test ./internal/formats -run '^$' -bench BenchmarkWriteSliceCSV -benchmem
~~~

On Windows, build bin/gengo.exe. A local dev.ps1 helper may provide build, test,
vet, bench, and run commands with the portable Go setup.

For a repeatable CLI and isolated database smoke check, use
[verify-gengo](.agents/skills/verify-gengo/SKILL.md). Its helper runs tests, vet,
and build, then generation, import, workloads, saved comparison, and verified
schema cleanup. Evidence stays in the printed temporary directory. Use its
explicit offline mode when database verification is not intended.

This checkout uses the tracked `.githooks/pre-commit` hook to run the full live
verification before each commit. Enable it in a fresh clone with
`git config --local core.hooksPath .githooks`. Go, psql, pgbench, and the three
database targets must be available. Optional local Git settings
`gengo.goExecutable`, `gengo.goPath`, and `gengo.goCache` support portable Go
installations. Verification failure blocks the commit; do not bypass it to report
an unverified commit as checked. The hook verifies the working tree, so include
the intended changes in the index before committing.

TestE2E builds its own binary and exercises all eight model/format combinations.
Its default target is 0.1 GiB per case. GENGO_TEST_SIZE_GB and GENGO_TEST_TIMEOUT
adjust fixtures. GENGO_KEEP_OUTPUT=1 retains output, so repeated runs need fresh
destinations. CI defines Linux and Windows checks, with race checks on Linux.

## Contracts to preserve

- Resolve defaults once per run. Explicit seed zero is valid.
- Use generation.Config streams and table-owned faker instances in active paths.
  Stream labels and derivation are part of generation.AlgorithmVersion.
- Workers execute stable logical partitions. A fixed resolved plan produces
  identical CSV data files and equivalent Parquet schemas and values across
  worker counts. Partition size can affect dataset identity and generated values.
- Compare formats with the same explicit row plan. Compression-aware size planning
  can otherwise resolve different counts.
- Preserve complete primary-key, foreign-key, customer-correlation, and count
  checks. Keep financial price state across file boundaries.
- Retail date keys are Unix days. Zero closing-date keys denote open locations.
  E-commerce CSV order timestamps use Unix seconds. Parquet timestamps use
  microseconds. See docs/reproducibility.md for the full contract.
- Register counts after writers close successfully. Manifest identity excludes
  workers and timings. Preserve staging and empty-destination publication.
- Reuse current model structs and metadata when extending imports. The e-commerce
  CSV timestamp override is explicit in internal/schema/ecommerce.go.

## Working approach

1. Inspect git status and the relevant normal entry point.
2. Read the implementations and tests involved in the change.
3. Trace the serialization and generation paths callers actually use.
4. Implement a bounded increment and check its normal CLI path where relevant.
5. Update feature documentation and the product checklist with verified status.
6. Review the diff, run appropriate checks, and commit focused changes.

Keep generated datasets, binaries, profiles, local setup helpers, and
machine-specific benchmark logs outside commits. Format changed Go files.
Algorithm or schema changes need corresponding version and documentation updates.
Current repeat-run tests compare the same implementation with itself, so review
compatibility with earlier versions separately.

Introduce runner contracts alongside working execution paths. The agreed first
benchmark engine is pgbench, with a native Go runner as a later direction.
Credentials belong in supported environment or credential sources.

## Current verification gaps

Live PostgreSQL imports passed against Supabase PostgreSQL 17.12. Opt-in tests
cover counts, constraints, joins, existing-schema rejection, and failed-load
rollback. Set GENGO_POSTGRES_TEST_ENV_FILE to an absolute dotenv path to run them.
Direct, shared transaction, and dedicated transaction connection checks passed.
Read-only pgbench customer-lookup sweeps passed across all three targets at one
and two clients. Temporary libpq hostaddr overrides avoided intermittent DNS
failures during that smoke verification; stored credentials were unchanged.
Offline saved-run comparisons validate configuration and measurement consistency;
their tests and CLI verification do not require a database or credentials.
Read-only custom SQL supports scenario-relative files, optional datasets, and
SHA-256 workload identity in saved results and comparisons.
Custom read/write scenarios add setup SQL and reset-per-phase or declared reuse.
Lifecycle scripts are snapshotted, hashed, and timed separately in state.go;
setup/reset failures stop the experiment. See docs/write-workloads.md.
Live write/reset sweeps passed on all three targets. Database audit counts prove
warm-up isolation and restored baselines; reuse and transactional failure checks
passed on the direct target.
Live custom sweeps and offline comparisons passed on all three endpoints;
table-read, read-only write rejection, and SQL-error continuation checks passed.
Arrival rates, connection experiments, repetitions, and available Supabase metrics
now support these sweeps. The local PostgreSQL/PgBouncer fixture remains optional.

Poisson arrival-rate sweeps support fixed clients, requested rates, a latency
limit for skipping overdue work, and explicit skipped-rate and rate-attainment
thresholds. Metrics reconcile scheduled/completed/failed/skipped counts, label
scheduled-start latency, and record successful-iteration scheduling lag.
Live low-rate/overload checks passed on all three endpoints. Arrival scheduling
also passed direct-endpoint write/reset, reuse, and rollback checks. See
docs/arrival-rates.md.

Connection modes are persistent, reconnect, and burst; protocols are simple,
extended, and prepared. All 54 live mode/protocol/target/load combinations passed.
Direct audit rows prove session reuse/reconnection and bounded burst counts.
Reconnect TPS includes connection times; unpaced log percentiles exclude each
client's own setup. Average connection time is separate. Bursts run one iteration
per client with one thread per client and use duration as a deadline. See
docs/connection-experiments.md.

Repetitions and fixed/rotating endpoint rounds preserve each sample and report
min/median/max values. Capacity defaults to all repetitions passing; an explicit
min_pass_rate can relax threshold failures, while missing samples and execution
errors never qualify. Repeated burst sweeps passed across all three endpoints;
direct write audit checks passed reset isolation, reuse, and rollback over three
repetitions. See docs/repeated-experiments.md.

Quick/thorough profiles resolve omitted warm-up, duration, repetition, endpoint
order, and concurrency settings before validation. Explicit overrides remain
authoritative; results record profile name/version and resolved controls.
verify-gengo -profiles runs both full defaults on the same 1,000-customer fixture
across requested endpoints. See docs/experiment-profiles.md. Built-in write
presets with managed state restoration remain open.

Supabase Metrics API collection uses SUPABASE_API_KEY, including sb_secret keys,
with a project-wide initial snapshot and one-minute scrapes. Results record UTC
scrape/phase times, availability, and checksummed Prometheus files. Telemetry
failures do not fail experiments. Use verify-gengo -require-metrics for live
snapshot/checksum checks and a 65-second periodic-collection proof. See
docs/supabase-metrics.md.

Dimensions remain resident. Worker limits are per fact table, and retail CSV
writers allocate 64 MiB buffers. Process-wide budgets and generation cancellation
are further work; PostgreSQL execution supports context cancellation and Ctrl+C.
cmd/profile launches a fixed 10 GiB generation run, so choose profiling inputs
deliberately.
