# Run controlled PostgreSQL workloads

Run versioned scenarios against direct and pooled connections, observe saved
measurements, and verify write/reset effects independently in the database.

## Sub-features

- `bench-lookup`: built-in customer lookup, repetitions, and rotating endpoints.
- `bench-custom`: scenario-relative custom SQL and lifecycle snapshots/hashes.
- `bench-write`: reset-per-phase writes, warm-up isolation, reuse, and rollback.
- `bench-load`: concurrency and Poisson arrival-rate controls.
- `bench-connect`: persistent, reconnect, burst, and simple/extended/prepared protocols.

## How to get to it (user POV)

Run `gengo bench run --scenario scenario.yaml --env-file .env --output results`.
Scenarios are versioned YAML; workload and lifecycle files are separate SQL files.

## Driving it with verify-gengo

Preconditions: Go/psql/pgbench, direct credentials, and credentials for requested poolers.

- Run `go run ./.agents/skills/verify-gengo/scripts -env-file .env -targets direct,shared,dedicated`.
- Inspect `lookup.yaml`, `baseline/result.json`, and `candidate/result.json`.
  Each experiment has two repetitions per endpoint, one client, simple protocol,
  and rotating endpoint order. Require completed measurements and zero failures.
- Inspect `burst.yaml` and `burst/result.json`. Prepared-protocol bursts execute
  exactly one iteration per client on each target.
- Inspect `write.yaml`, lifecycle SQL snapshots, and `write/result.json`.
  The extended-protocol write runs five requested arrivals per second with one
  client, one second of warm-up, and two seconds of measurement per endpoint.
- Read `write-audit.sql/log` and `write-restored.log`. The audit stores the counter
  before warm-up, after warm-up, and after measurement. Warm-up must write rows;
  measured writes must match the saved successful iterations. The final counter
  must be zero. Warm-up phases record status and time, not iteration metrics.
- For the wider existing live matrix, set `GENGO_POSTGRES_TEST_ENV_FILE` to an
  absolute dotenv path and `GENGO_POSTGRES_TEST_TARGETS=direct,shared,dedicated`,
  then run `go test ./tests -run 'Live$' -count=1 -v -timeout=20m`.
  Retain the transcript. This additionally exercises overload/threshold failures,
  reconnect, all protocols, declared reuse, SQL errors, repetitions, and rollback.
  A skipped test does not count as verified. Unset the opt-in variables afterward.

## Gotchas

- The short helper is a functional smoke check, with generous latency thresholds.
  It is not a capacity result or exhaustive load/protocol matrix.
- Threshold failures can save a completed experiment while returning a nonzero
  command status. The smoke helper requires every selected step to pass.
- Bursts require zero warm-up. Arrival-rate latency includes scheduling delay.
- Cancellation and abrupt termination need separate verification when relevant;
  they are not established by a normal successful run.
