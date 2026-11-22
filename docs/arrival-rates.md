# Run workloads at a target arrival rate

Use `arrival_rates` with a fixed `clients` count to sweep requested workload
iterations per second. Start with `examples/postgres/arrival.yaml`, then replace
its SQL with queries against the database you want to measure:

```sh
gengo bench run --scenario examples/postgres/arrival.yaml \
  --env-file .env --output data/arrival-run-001
```

Replace a scenario's `concurrency` field with these controls:

```yaml
clients: 4
arrival_rates: [5, 10, 20]
latency_limit: 500ms
```

Add these thresholds alongside `max_p95_ms`, `max_error_rate`, and `min_tps`:

```yaml
max_skipped_rate: 0.01
min_rate_attainment: 0.9
```

`min_rate_attainment` is required, greater than zero and at most one. It is
successful TPS divided by requested rate. `max_skipped_rate` defaults to zero
and accepts values between zero and one. Skipped work and throughput shortfalls
therefore cannot pass based on latency alone.

Rates must increase, with 1 to 32 levels between 0.1 and 1,000,000 iterations per
second. Fractional rates are supported. `clients` must be between 1 and 1024,
and `latency_limit` must be between `1ms` and `30s`. These are input bounds,
not recommended traffic levels. Rate controls and `concurrency` are mutually
exclusive. Existing concurrency scenarios keep their original behavior.

## Scheduling and latency

Gengo uses pgbench `--rate` and `--latency-limit`. pgbench generates a Poisson
arrival schedule. Clients follow scheduled start times instead of immediately
starting their next iteration after completion. Client count stays fixed across
the rate sweep. Capacity depends on both database performance and the configured
client count and single pgbench worker thread.

When a client falls behind schedule by more than `latency_limit`, pgbench skips
that iteration without sending SQL. The limit does not cancel slow SQL and is
not a statement timeout. Executed iterations can complete after the limit.
Separate timeout counts remain unavailable.

Rate-mode latency runs from scheduled start to completion. It includes client
scheduling lag, script processing, network effects, and database work. It is not
isolated server query latency. Concurrency mode measures iteration latency without
a target arrival schedule. The two load models cannot be compared as equivalent
experiments.

Each rate level has separate warm-up and measurement processes at the same target
rate. [Setup and reset policies](write-workloads.md) also work with arrival rates,
including a fresh reset before measured write traffic. Normal phase timeouts,
cancellation, and failure continuation remain unchanged.

## Measurements and thresholds

Results use `load_model: poisson-arrival-rate`. Each step records its
`arrival_rate` and actual client count in `concurrency`. The summary shows
requested rate, successful TPS, scheduled-start p95, failures, skipped iterations,
and achieved percentage of the requested rate.

| Field | Meaning |
| --- | --- |
| `requested_iterations` | Observed scheduled iterations, reconciled as completed plus failed plus skipped. |
| `completed_iterations` | Successfully completed workload iterations. |
| `failed_iterations` | Executed iterations that pgbench reports as failed. |
| `skipped_iterations` | Scheduled iterations never sent because they were already too late. |
| `skipped_rate` | Skipped divided by requested iterations. |
| `rate_attainment` | Successful TPS divided by requested iterations per second. |
| `successful_iteration_schedule_lag_mean_ms` | Mean scheduling delay for successful iterations. |
| `successful_iteration_schedule_lag_max_ms` | Maximum scheduling delay for successful iterations. |
| `latency_basis` | `scheduled-start-to-completion` for rate measurements. |

Latency percentiles and scheduling-lag statistics exclude failed and skipped
iterations. Error rate retains its existing definition, failed divided by
completed plus failed. Skipped work has its own threshold and is not relabeled
as an SQL error or timeout. All existing thresholds apply alongside the new ones.

Gengo reconciles completed, failed, and skipped log rows against the pgbench
summary. Requested count is not rate multiplied by duration. Poisson scheduling,
connection setup, and the phase cutoff affect observed work. Short samples can
achieve above or below the target even without overload. Increase duration when
judging sustained capacity. TPS excludes initial connection time. A phase without
successful iterations is an execution error, since no successful-latency
percentile can be calculated.

`capacity_arrival_rate` records each target's highest tested rate passing all
thresholds, in iterations per second. Client capacity is unset for rate runs.
Zero means no measured rate passed; preflight failures have no capacity entry.
Capacity does not estimate untested levels or establish saturation when every
tested level passes.

## Compare saved rate experiments

Use `gengo bench compare BASELINE_JSON CANDIDATE_JSON`. Load model, ordered rate
levels, fixed client count, latency limit, thresholds, and existing workload and
state settings must match. Equivalent duration spellings match. Comparisons pair
steps by target, client count, and requested rate. They report TPS, p95, errors,
skipped-rate changes, rate attainment, and tested rate capacity. Saved comparison
JSON separates `capacity_arrival_rate` from client `capacity`.

Comparison validates scheduled-work count arithmetic and ratios, requires all
rate measurements, and recalculates threshold status and capacity. Existing
saved concurrency results remain supported.

## Verification

```powershell
$env:GENGO_POSTGRES_TEST_ENV_FILE = (Resolve-Path .env).Path
$env:GENGO_POSTGRES_TEST_TARGETS = 'direct, shared, dedicated'
go test ./tests -run '^TestArrivalRateLive$' -count=1 -v

$env:GENGO_POSTGRES_TEST_TARGETS = 'direct'
$env:GENGO_POSTGRES_TEST_LOAD_MODEL = 'arrival'
go test ./tests -run '^TestWriteResetLive$' -count=1 -v
```

On 22 November 2026, live CLI tests passed low-rate and deliberately overloaded
steps on direct, shared Supavisor, and dedicated PgBouncer connections. Saved
results reconciled all counts and compared offline. Direct-endpoint write tests
also passed with arrival scheduling, including reset isolation, retained writes
under reuse, and setup/reset/workload rollback. These runs verify functionality,
not representative database capacity.

Build, vet, and the full Windows suite passed with `GENGO_TEST_SIZE_GB=0.00001`
for the generation fixtures. Unit and process tests cover strict rate scenarios,
exact skipped-work and scheduling-lag calculations, threshold equality,
continuation across failed rates, missing measurements, and comparison matching.
