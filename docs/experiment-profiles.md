# Use quick and thorough experiment profiles

Add `profile: quick` or `profile: thorough` to a version-1 scenario. A profile
supplies omitted experiment controls; explicit scenario values override them.

| Version 1 default | quick | thorough |
| --- | --- | --- |
| Warm-up per sample | 1s | 3s |
| Measurement per sample | 5s | 15s |
| Repetitions | 1 | 3 |
| Endpoint order | fixed | rotate |
| Concurrency levels | 1, 2, 4 | 1, 2, 4, 8 |

Quick supports short functional and regression checks. Thorough records variation
across repetitions and changes which endpoint runs first. With three targets,
the default workload phases take 54 seconds for quick and 648 seconds for
thorough. Connection checks, process startup, setup/reset, and telemetry add time.
These settings are starting points; the profile name does not establish that a
measurement window or dataset is suitable for your application.

```yaml
version: 1
name: customer-lookup-quick
profile: quick
dataset: ../../data/ecommerce
schema: gengo
targets: [direct, shared, dedicated]
workload: customer-lookup
seed: 42
protocol: simple
thresholds:
  max_p95_ms: 250
  max_error_rate: 0
  min_tps: 1
```

```sh
gengo bench run --scenario examples/postgres/quick.yaml \
  --env-file .env --output data/quick-001
```

Use `examples/postgres/thorough.yaml` for the thorough defaults. Dataset paths
are relative to each scenario. Set your schema, targets, workload, seed, protocol,
and thresholds explicitly; profiles do not select them or change credentials.

## Override individual settings

```yaml
profile: thorough
warmup: 0s
duration: 30s
concurrency: [2, 8, 16]
```

This retains three repetitions and rotating endpoint order, skips warm-up,
and measures each listed load for 30 seconds. Empty or invalid explicit controls
are validated as usual. Without a profile, the existing explicit-scenario rules
are unchanged.

For arrival-rate experiments, supply `clients`, `arrival_rates`, `latency_limit`,
and rate thresholds as usual. Profiles do not inject concurrency into an
arrival-rate sweep or choose application-specific rates. Connection bursts
require an explicit `warmup: 0s` override.

## Read and compare saved results

The terminal summary identifies the profile version and resolved timings,
repetitions, and ordering. Saved scenarios record `profile`, `profile_version`,
and the resolved controls. Reading results never reapplies profile defaults.
Changing named defaults requires a profile version change; YAML cannot supply
`profile_version`.

`bench compare` checks the resolved experiment controls. Default quick and
thorough results have different durations, repetitions, and load levels, so they
are incompatible for that command. Results with identical resolved controls can
compare even if their profile names differ; a warning records that difference.
Older results without profiles remain supported.

## Run both profiles through the real CLI

```sh
go run ./.agents/skills/verify-gengo/scripts -env-file .env \
  -targets direct,shared,dedicated -profiles -require-metrics
```

The helper generates a seeded 1,000-customer e-commerce fixture, imports it into
an isolated schema, runs the normal smoke checks, and executes both profiles
without overriding their controls. Profile workloads are read-only customer
lookups in simple protocol with seed zero. Their thresholds are p95 at most
250 ms, zero errors, and at least one iteration per second. Threshold failures
remain in results and do not prevent the other profile from running; execution
failures fail verification. All repetitions must pass to qualify a load level.

Inspect `profile-quick/result.json` and `profile-thorough/result.json` in the
printed evidence directory. `profiles_verified: true` means both profiles
executed completely, not that every threshold passed. The helper verifies schema
cleanup and retains result files and telemetry. Omitting `-require-metrics`
leaves telemetry optional as in ordinary benchmark runs.

Create a local interactive report from those saved results without database access:

```sh
go run ./.agents/skills/verify-gengo/scripts -report-profiles PATH_TO_EVIDENCE
```

This writes `profile-report.html` and `profile-report.json` beside the original
results. The report shows each profile's throughput, median p95, and observed
min/max variation by endpoint and client count. It does not treat the two profiles
as compatible regression comparisons or infer production capacity from the small
fixture.
