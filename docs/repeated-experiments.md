# Repeat experiments and rotate endpoint order

Add repetition controls to an existing workload scenario:

```yaml
repetitions: 3
target_order: rotate
min_pass_rate: 1
```

Start with `examples/postgres/repeated.yaml`, replace the example SQL with your
workload, and run:

```sh
gengo bench run --scenario examples/postgres/repeated.yaml \
  --env-file .env --output data/repeated-001
```

The defaults remain one repetition, fixed endpoint order, and every repetition
passing. `repetitions` accepts 1 to 30. The complete experiment is limited to
1024 target/load/repetition samples. Each sample has its own warm-up and measured
traffic, so total runtime grows with the repetition count. Burst samples have
no warm-up and retain their individual process deadlines.

## Endpoint scheduling

Each repetition visits all targets and runs their increasing load levels.
`target_order: fixed` retains the listed target order for every repetition.
`target_order: rotate` moves the first target to the end after each round:

| Repetition | Order for `[direct, shared, dedicated]` |
| --- | --- |
| 1 | direct, shared, dedicated |
| 2 | shared, dedicated, direct |
| 3 | dedicated, direct, shared |

The pattern repeats for later rounds. Every complete cycle gives each target
each position once. Partial cycles can differ by one visit per position. This
is a deterministic rotation, independent of the workload seed. It distributes
first/last-position effects without eliminating background traffic or cache bias.

`schedule` in the saved result records the planned rounds. Individual `steps`
retain execution order and a one-based `repetition` identifier. With multiple
repetitions, artifact directories include `rep-N-`, for example
`rep-2-shared-c1-measurement`. Single-repetition directory names stay unchanged.
Original single-run saved results remain valid comparison inputs.

## Warm-up and mutable state

Connectivity checks and optional setup SQL run once per target per experiment.
They do not run again at the start of each repetition. Every sample runs its own
warm-up and measurement processes. The workload seed is reused across samples;
timing and concurrent execution are still not deterministic.

For `reset-per-phase`, each repetition resets before warm-up, before measurement,
and after the step. Warm-up writes and earlier repetitions therefore do not
become the next measured starting state when the reset script restores the
declared baseline. For `reuse`, writes deliberately carry across phases,
repetitions, and targets. See [write workload policies](write-workloads.md).

Setup/reset failure stops the experiment. Threshold failures and workload errors
retain the existing continuation behavior. Cancellation saves partial results
and does not launch new reset SQL. Repetition statistics never qualify an
incomplete load level for capacity.

## Aggregate statistics and capacity

Every target/load combination has an `aggregates` entry containing:

- Expected, measured, passed, failed, and errored repetition counts.
- Pass rate, using the configured repetition count as the denominator.
- Minimum, median, and maximum successful TPS and per-repetition p95 latency.
- The same summaries for per-repetition error rates, plus skipped rates, rate
  attainment, and average connection times where measured.

Each repetition contributes one value to each available statistic. The displayed
p95 median is the median of the individual repetitions' p95 values. It is not a
pooled p95 over all transactions. Error-rate medians are also summaries of
individual rates, rather than ratios of pooled counts. Each original measurement
and its raw logs remain available for inspection.

A load qualifies only after all configured repetitions finish and the fraction
passing every per-sample threshold reaches `min_pass_rate`. The default of one
requires every repetition to pass. For example, with five repetitions,
`min_pass_rate: 0.8` requires at least four to pass. The setting must be greater
than zero and at most one. Capacity remains the highest tested qualifying load,
in clients, requested arrival rate, or burst size according to the load model.

Execution and reset errors always disqualify that target/load combination,
regardless of its pass rate. Completed measurement phases can still contribute
descriptive statistics, but a missing or errored repetition cannot be replaced
with an invented zero-throughput sample.

The experiment exits successfully only when all target/load groups meet their
pass rule and no preflight/lifecycle failure occurs. With a relaxed pass rule,
some individual steps may fail thresholds while the experiment finishes as
`completed`. A sweep reaching a load that fails the aggregate rule still exits
nonzero, while retaining its lower tested capacity and all samples.

## Compare repeated experiments

Use `gengo bench compare BASELINE_JSON CANDIDATE_JSON`. Repetition count,
endpoint-order policy, base target order, and minimum pass rate must match,
alongside the existing workload, state, and load-control settings.

The reader verifies repetition identifiers, complete sample coverage, the saved
schedule, aggregate statistics, pass counts, capacity, and overall status against
the individual measurements. Missing or inconsistent repeated results are
rejected. Legacy one-repetition results without aggregate metadata are supported.

Comparisons show differences between repetition medians, aggregate statuses,
and passing counts. Baseline and candidate references in comparison JSON retain
the complete aggregate distributions and schedules. These summaries describe
variation; they do not establish statistical significance or remove differences
in endpoints, hardware, pool sizing, or background activity.

## Verification

```powershell
$env:GENGO_POSTGRES_TEST_ENV_FILE = (Resolve-Path .env).Path
$env:GENGO_POSTGRES_TEST_TARGETS = 'direct, shared, dedicated'
go test ./tests -run '^TestRepetitionsLive$' -count=1 -v

$env:GENGO_POSTGRES_TEST_TARGETS = 'direct'
$env:GENGO_POSTGRES_TEST_REPETITIONS = '3'
go test ./tests -run '^TestWriteResetLive$' -count=1 -v
```

The first test checks passing and deliberately failing three-round burst sweeps,
actual endpoint rotation, individual artifacts, aggregate capacity, and offline
comparison. The second checks per-repetition warm-up/reset isolation against
database audit rows, state reuse, and transaction rollback. Unit/process tests
also cover strict and relaxed pass rules, exact odd/even medians, missing samples,
inconsistent schedules and aggregates, and lifecycle order.

On 22 November 2026, both three-round CLI burst sweeps passed their expected
passing/failing checks across direct, shared Supavisor, and dedicated PgBouncer
endpoints at one and two clients. Offline aggregate comparisons passed. Direct
PostgreSQL audit checks passed three-repetition writes, fresh measured baselines,
state reuse, and setup/reset/workload rollback. Build, vet, and the full Windows
suite passed with `GENGO_TEST_SIZE_GB=0.00001` for generation fixtures. These
short runs verify functionality, not database capacity or statistical confidence.

Named quick/thorough profiles remain separate work. Repetitions and endpoint
ordering are explicit scenario controls and do not change phase durations.
