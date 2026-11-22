# Run connection experiments

Set `connection_mode` in a workload scenario to compare persistent clients,
reconnection overhead, or connection bursts across configured endpoints:

| Mode | Execution |
| --- | --- |
| `persistent` | Default. Each client keeps its connection for the phase. |
| `reconnect` | pgbench reconnects before every executed workload iteration. |
| `burst` | One iteration per client, with one pgbench thread per client to initiate connections concurrently. |

Start with `examples/postgres/reconnect.yaml` or `connection-burst.yaml`. Replace
the example SQL with your workload, then run:

```sh
gengo bench run --scenario examples/postgres/reconnect.yaml \
  --env-file .env --output data/reconnect-001

gengo bench run --scenario examples/postgres/connection-burst.yaml \
  --env-file .env --output data/burst-001
```

Target labels select the existing direct, shared Supavisor, and dedicated
PgBouncer connection variables. Labels do not independently verify pooler type,
pool size, or backend configuration. Client connections and database backend
connections remain separate concepts. In particular, reconnecting through a
pooler does not imply a new database backend for every iteration.

## Select a protocol

`protocol` accepts `simple`, `extended`, or `prepared`. Keep it explicit in each
scenario. Prepared mode prepares statements for reuse on each client connection;
reconnecting discards that client session's prepared state. A transaction pooler
must support the selected protocol and workload. Gengo does not silently fall
back to another protocol when an endpoint rejects it. Saved phase errors and
redacted pgbench diagnostics preserve the failure.

Setup/reset scripts continue to use psql independently of the workload protocol.
Workload SQL snapshots and hashes remain identical when only protocol or
connection mode changes.

## Run reconnect traffic or bursts

Reconnect mode supports both concurrency and arrival-rate sweeps, using pgbench
`--connect`. Persistent and reconnect traffic use one pgbench worker thread.
That thread's connection work can limit achieved load and delay other clients.
Skipped rate-mode iterations do not open workload connections.

Bursts use the `concurrency` list as increasing burst sizes, limited to 128
clients. Each phase starts a fresh pgbench process with `--connect`, one thread
per client, and exactly one iteration per client. This is a best-effort concurrent
start, not a guarantee that all connections arrive at the server simultaneously.
Each level runs one burst. Repeated bursts are separate runs until repetition
support is added.

Burst scenarios require zero warm-up and do not accept arrival rates. Their
`duration` is a deadline for the entire burst process, rather than a traffic
duration. A burst that exceeds the deadline is an errored phase. The load model
is `connection-burst`. `requested_iterations` equals the burst's client count;
completed plus failed counts must match before the phase can be compared.

Custom write workloads can use setup, reset, or declared reuse with these modes.
For reset-policy bursts, Gengo resets before the burst and after the step.
Setup/reset failures stop the experiment; workload errors retain the existing
reset and continuation behavior. Cancellation stops work without launching new
reset SQL. See [write workloads](write-workloads.md).

## Read connection measurements

pgbench reports reconnect and burst TPS **including connection time**. Results
store it as `tps_including_connection_times`, with
`throughput_basis: including-connection-times`. Their elapsed interval is
`pgbench_elapsed_seconds_including_connection_times`. The existing fields for
TPS and elapsed time without initial connection setup are omitted for these
modes, rather than filled with values having a different meaning.

`average_connection_ms` is pgbench's average connection and disconnection time
per executed iteration. It is separate from transaction latency. Initial
connection time and connection-setup percentiles are unavailable for reconnect
and burst measurements, and are listed as such in the result.

For unpaced reconnects and bursts, transaction log latency starts **after that
client's own connection is established**. Results label it
`transaction-start-to-completion`. Mean and p50/p95/p99 therefore exclude that
client's own setup. They can include delays caused by other clients, network
traffic, and database work. An end-to-end connection-plus-iteration p95 cannot
be reconstructed by adding average connection time to query p95; Gengo lists
that percentile as unavailable.

For rate-mode reconnects, latency still runs from scheduled start to completion.
Connection setup is included in scheduling lag. Successful-iteration lag
statistics therefore include connection work as well as scheduling delay.
Failed connection attempts can abort a client/process before a complete log row
exists. Such phases remain errors; Gengo does not invent per-attempt failure
counts or compare them as successful zero-throughput runs.

The existing `min_tps` threshold uses the applicable throughput field. Optional
`max_connection_mean_ms` applies to reconnect and burst modes and fails a step
when average connection time exceeds the limit. Zero disables that threshold.
`max_p95_ms` continues to apply to the labeled transaction latency. Use connection
time or throughput criteria when judging connection overhead.

Burst capacity is the highest tested burst size passing all thresholds, saved
in `capacity_clients`. Sustained concurrency and arrival-rate capacity retain
their existing meanings. A small successful burst does not establish the
maximum connection limit of the server or pooler.

## Compare modes and protocols

Run separate scenarios with the same workload and load controls, changing
`connection_mode` or `protocol`, then use `bench compare`. Intentional protocol
and persistent/reconnect changes are allowed and called out in comparison
warnings. Baseline and candidate references record their effective connection
mode and protocol. Throughput and transaction-latency bases remain explicit.

Burst results compare only with other burst results. They cannot compare with
duration-driven concurrency or arrival-rate runs. All other compatibility checks,
including thresholds, setup/reset hashes, load levels, and target order, remain
in force. Connection behavior and protocol are not swept within one scenario.

## Verification

```powershell
$env:GENGO_POSTGRES_TEST_ENV_FILE = (Resolve-Path .env).Path
$env:GENGO_POSTGRES_TEST_TARGETS = 'direct, shared, dedicated'
go test ./tests -run '^TestConnectionExperimentsLive$' -count=1 -v
go test ./internal/benchmark -run '^TestConnection(Metrics|Identity)Live$' -count=1 -v
```

On 22 November 2026, all 54 CLI steps passed across the three modes, three
protocols, three endpoints, and one/two-client levels. Saved results compared
offline. Direct PostgreSQL audit rows separately confirmed persistent session
reuse, a new session per reconnect iteration, one session per burst client,
and reconnect arrival-rate writes. Pooled backend identities were not used to
infer client connection counts. These checks verify functionality, not endpoint
capacity or representative performance.

Build, vet, and the full Windows suite passed with `GENGO_TEST_SIZE_GB=0.00001`
for generation fixtures. Unit/process tests cover protocol and mode validation,
exact command arguments, timing labels, connection-time thresholds, bounded
burst counts, persisted deadline errors, and intentional comparison changes.
