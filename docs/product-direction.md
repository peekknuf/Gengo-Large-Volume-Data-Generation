# Gengo product direction

Agreed 21 November 2026.
Workload experiment priorities updated 22 November 2026.

## Purpose

Gengo will generate reproducible datasets, prepare test environments, run controlled
workloads, and report how capacity changes under load. PostgreSQL is the first
target. The long-term direction covers Supabase services and complete application
workflows involving Auth, Storage, Supavisor, Edge Functions, Realtime, and other
Supabase features.

The initial execution engine is pgbench. A native Go workload runner is the
preferred longer-term direction. Gengo owns scenario definitions, dataset
identities, experiment scheduling, thresholds, and result formats.

## Agreed product decisions

| Area | Decision |
| --- | --- |
| Primary outcome | Capacity curves across increasing concurrency, comparing direct and pooled connections with throughput, latency percentiles, errors, and timeouts. |
| Workloads | Custom SQL files and built-in point-lookup, mixed read/write, and join/aggregation presets. |
| Connection behavior | Persistent clients first, followed by reconnect-per-transaction traffic and connection bursts. |
| Targets | Existing Supabase direct, shared Supavisor, and dedicated PgBouncer endpoints first. A local PostgreSQL and PgBouncer reference environment is optional later work. |
| Execution | pgbench for the first version, with Gengo-owned contracts that a future Go runner can reuse. |
| Load controls | Both concurrency-driven load and target arrival rates. Reports identify the load model and achieved rate. |
| Telemetry | Client measurements on every run. PostgreSQL and pooler telemetry when access permits, with availability recorded. |
| Dataset ownership | Dedicated schemas for generated datasets and built-in workloads. Custom SQL can target existing application schemas. |
| Results | Saved JSON results, a readable summary, and comparisons between saved experiments. |
| Experiment profiles | Quick and thorough profiles with explicit warm-up, measurement duration, repetitions, and load steps. Thorough comparisons vary endpoint order. |
| Mutable data | Starting-state policy recorded for every cycle. Built-in write workloads restore Gengo-managed state. Custom workloads can provide setup/reset SQL or declare state reuse. |
| Parameters | Seeded uniform selection, configurable hot subsets, and weighted workload mixes. |
| SQL protocols | Explicit simple, extended, and prepared modes, with simple as the initial default. |
| Success criteria | Scenario-defined latency, error-rate, and achieved-throughput criteria. Capacity is the highest tested level meeting them. |
| Scenario authoring | Version-controlled YAML and separate SQL files. |
| Failure behavior | A threshold breach marks a step as failed. The experiment continues through its configured levels unless a separate stop condition is reached. |
| Delivery | One complete PostgreSQL workflow first, followed by broader model, workload, and service coverage. |

## Measurement contract

A workload iteration is one execution of a scenario action or script. SQL
transactions and individual statements receive their own labels where measured.
Requested work, completed work, errors, timeouts, and skipped work must be
distinguishable in the result format.

Client connections, pooler backend connections, and executing database queries
are separate measurements. Connection setup, client-side queuing, pooler waiting,
SQL execution, and end-to-end latency retain their own labels. Reports must state
which layers were measured and which were unavailable.

Dataset creation, import, indexes, foreign-key validation, statistics collection,
warm-up, measured traffic, and reset operations have separate timings. Each result
records its seed, dataset identity, workload version, endpoint mode, protocol,
configuration, and available software versions.

## First usable PostgreSQL workflow

1. Generate the existing e-commerce model with a fixed seed and row plan.
2. Prepare a dedicated PostgreSQL schema and load the CSV shards.
3. Build primary-key indexes, validate foreign keys, verify counts, and analyze.
4. Run a point-lookup preset or custom SQL against direct and PgBouncer endpoints.
5. Sweep concurrency with explicit warm-up and measurement periods.
6. Save a result and compare experiments using identical scenario settings.

## Current delivery priority

Focus on workload experiments against the existing Supabase connections. The
customer-lookup concurrency sweep and offline comparisons already work. Extend
that workflow in this order:

1. Add custom SQL files to the existing concurrency sweeps, saved results, and
   comparisons. Record workload identity so changed SQL cannot silently compare
   as the same workload.
2. Add read and write experiments with explicit starting-state policies. Support
   setup and reset SQL or declared state reuse for custom workloads. Built-in
   write workloads restore Gengo-managed state.
3. Add target arrival rates. Distinguish requested rate, achieved throughput,
   skipped work, errors, and available latency measurements.
4. Add connection behavior experiments, including reconnect-per-transaction
   traffic, connection bursts, and protocol comparisons where targets support
   them. Persistent clients remain the baseline.

Add repetitions and vary endpoint order as these experiments expand. Collect
available Supabase Metrics API measurements alongside client results, with
collection intervals and unavailable measurements recorded. Telemetry supports
the workload experiments without becoming a prerequisite for running them.

The local PostgreSQL and PgBouncer reference environment remains optional backlog
work. Shared generation worker and memory budgets and Linux/race verification
remain open, but do not gate current Supabase workload experiments. Generation
memory limits still matter when choosing larger dataset plans.

## Supabase expansion

Scenario concepts should accommodate actors, identities, actions, and phases.
The first concrete action is SQL execution. Later actions can cover authentication,
object uploads/downloads, Edge Function invocation, and Realtime subscriptions.
Shared Supavisor SQL connections are already supported. Broader Supabase service
scenarios remain later work.

Future experiments can combine these actions into application workflows under
increasing load. Transport-specific execution belongs to the relevant runner.
The shared scenario, scheduling, measurement, and result concepts stay reusable.
Concrete abstractions are introduced alongside working implementations.

## Delivery checklist

### Existing foundation

- [x] Integrity fixes and exact declared row counts.
- [x] Seeds, reference dates, stable fact partitions, and manifests.
- [x] Windows build, tests, and vet checks, including eight generation combinations.
- [x] Inactive generator framework and duplicate writer paths removed, with seeded output compatibility checked.
- [ ] Linux and race-check results verified.
- [ ] Shared worker and memory budgets for larger dataset generation.

### First PostgreSQL increment

- [x] Shared product direction recorded in this document.
- [x] E-commerce schema metadata derived from the current model structs.
- [x] Manifest-driven CSV import scripts with explicit phase boundaries.
- [x] CLI checks for generated import plans and failure cases.
- [x] Direct, shared transaction, and dedicated transaction endpoint checks with environment-based credentials.
- [x] CLI import execution with psql, cancellation, timeouts, and retained import scripts.
- [x] Live PostgreSQL import validation, including counts, constraints, joins, and failed-load rollback.
- [x] Versioned YAML scenario loading and validation for read-only customer lookups.
- [x] Point-lookup preset and pgbench execution using simple protocol.
- [x] Concurrency sweep, saved JSON results, client latency percentiles, thresholds, and readable report.
- [x] Offline saved-run comparisons with compatibility checks and observed metric/capacity changes.

### Current workload increments

- [ ] Custom SQL files with workload identity in saved results and comparisons.
- [ ] Read/write workloads with setup, reset, and explicit state-reuse policies.
- [ ] Target arrival-rate tests with requested and achieved load measurements.
- [ ] Connection churn, bursts, and supported protocol comparisons.
- [ ] Quick/thorough profiles, repetitions, and varying endpoint order.
- [ ] Available Supabase Metrics API collection alongside client measurements.

### Later increments

- [ ] Optional local PostgreSQL and PgBouncer reference environment.
- [ ] Weighted workloads and hot subsets.
- [ ] Additional PostgreSQL and pooler telemetry where access permits.
- [ ] Configurable stop conditions.
- [ ] Additional generation models and workload presets.
- [ ] Native Go execution engine.
- [ ] Supavisor, Auth, Storage, Edge Functions, and Realtime scenarios.

Checklist items are completed when the corresponding implementation and checks
are present. Planning a component leaves its implementation item open.
