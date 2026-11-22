# Collect Supabase metrics during experiments

Set `SUPABASE_API_KEY` to your project's `sb_secret_...` key in the process
environment or the dotenv file passed to `bench run`. Process variables take
precedence. The collector also accepts a legacy service-role key.

```dotenv
SUPABASE_API_KEY=replace-with-your-project-secret-key
```

Run your existing scenario:

```sh
gengo bench run --scenario scenario.yaml --env-file .env --output results/run-001
```

Gengo infers the project reference from the selected direct, shared, and dedicated
database URLs. For `libpq`, it uses `PGHOST` and `PGUSER`. Every selected target
must identify the same hosted Supabase project. Local, custom-hostname, and
mixed-project targets leave collection unavailable. If `SUPABASE_URL` is set,
it must match `https://<project-ref>.supabase.co`.

The collector uses HTTP Basic authentication with username `service_role` and
the configured key as its password. It calls the project's
`/customer/v1/privileged/metrics` endpoint over HTTPS and refuses redirects.
Credentials are absent from saved configuration, requests on command lines,
and diagnostic errors. Failed response bodies are discarded.

Collection defaults to `auto`. To disable it for an experiment, add this
top-level scenario field:

```yaml
supabase_metrics: off
```

## Inspect the observations

`result.json` contains a `supabase_metrics` object with the project reference,
collection interval, scope, status, and scrape attempts. Each successful attempt
records its UTC start/end times, HTTP status, sample count, snapshot filename,
and SHA-256 checksum. The full Prometheus exposition stays in adjacent
`supabase-metrics-000000.prom` files so metric names and labels remain available.
Responses are limited to 8 MiB and requests time out after ten seconds.

Gengo takes an initial snapshot before workload execution, then scrapes every
minute while the experiment runs. This follows the
[Supabase collection recommendation](https://supabase.com/docs/guides/observability/metrics).
Collection covers preflight, setup/reset, warm-up, and measured traffic. Each
workload and lifecycle phase records UTC start/end times for correlation.
The initial request runs before any pgbench phase, outside its client timing.

Experiments shorter than a minute usually have only the initial snapshot.
Use longer measurements when you need observations during load. Scrape times
are observation times; they do not establish the freshness of each underlying
metric. Metrics describe the entire project, including unrelated traffic and
other endpoints. They do not measure an individual step's query execution or
pooler wait time, and client connections remain a separate measurement.

The terminal summary reports collection availability. `available` means every
attempt succeeded; `partial` means some succeeded; `unavailable` means none did.
`disabled` means the key is absent or the scenario opted out. Missing or failed
telemetry never changes workload thresholds, capacity, or the CLI's success
status. Cancellation stops the collector and retains completed snapshots.

Saved comparisons retain each run's telemetry status and project reference.
They compare client measurements as before; telemetry snapshots remain in the
original run directories and do not contribute metric deltas or capacity rules.
Older results without telemetry remain readable.

## Verify the live integration

```sh
go run ./.agents/skills/verify-gengo/scripts -env-file .env \
  -targets direct,shared,dedicated -require-metrics
```

This checks the normal generation/import/workload/comparison flow and requires
successful snapshots with matching checksums. It adds a 65-second direct-endpoint
read-only experiment at one requested iteration per second, requiring at least
two snapshots to prove periodic collection. Inspect `metrics/result.json` and
its `.prom` files in the printed evidence directory. The helper verifies schema
cleanup and retains evidence.
