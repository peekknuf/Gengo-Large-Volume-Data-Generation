# Run read/write experiments with setup and reset SQL

Custom SQL scenarios support `access_mode: read-write` with an explicit
`state_policy`. Start with `examples/postgres/custom-write.yaml`. Its setup
creates an example accounts table in the dedicated `gengo_write_example` schema.
The workload reads an account and increments its balance. Reset restores all
balances to zero.

These state policies also support [arrival-rate sweeps](arrival-rates.md).
Replace `concurrency` with fixed `clients`, `arrival_rates`, and `latency_limit`,
and add the required `min_rate_attainment` threshold.

```sh
gengo bench run --scenario examples/postgres/custom-write.yaml \
  --env-file .env --output data/write-run-001
```

Adapt all three SQL files to the tables owned by your experiment. Each workload
iteration runs in `BEGIN READ WRITE` and `COMMIT`. Multi-statement iterations
commit together, and SQL errors roll back that iteration. Previous successful
iterations remain committed until reset. Do not put transaction commands in any
of these scripts.

## Select a starting-state policy

| `state_policy` | Behavior |
| --- | --- |
| `read-only-reuse` | Default for existing scenarios. Requires read-only access and no setup/reset files. |
| `reset-per-phase` | Requires `reset_file`. Resets before warm-up, before measurement, and after each load step. |
| `reuse` | Explicitly carries state through warm-up, measurement, load levels, and targets. Does not accept `reset_file`. |

Read/write access requires `reset-per-phase` or `reuse`. A read-only workload
may also use these policies, for example to prepare starting data with setup SQL.
The built-in customer lookup remains read-only without lifecycle scripts.

Optional `setup_file` runs once per target, after connectivity checks and before
any load steps. Targets can point to the same database, so write setup scripts
that can run repeatedly. Setup does not run before every measurement; restoring
the baseline is the reset script's job.

With `reset-per-phase`, one load step runs in this order:

1. Run reset, then warm-up, when `warmup` is greater than `0s`.
2. Run reset, then measured traffic.
3. Run reset again to leave the baseline restored.

Warm-up writes therefore do not become measurement's starting data. A failed
warm-up skips measurement and still runs the final reset. A workload error or
threshold breach also runs the final reset before continuing the sweep. A
setup/reset error stops the entire experiment because its starting state is no
longer established. The result retains completed phases and the failing phase.

Cancellation stops active work and saves a canceled result. It does not launch
new reset SQL after cancellation, so committed writes may remain. Restore the
baseline before restarting. The same applies after a process crash. Write setup
and reset SQL that converges to the intended state on repeated execution.

## Author lifecycle scripts

`setup_file` and `reset_file` resolve relative to the scenario YAML and have the
same nonempty, regular-file, 1 MiB limit as workload SQL. Gengo snapshots all
three files when loading the scenario. Edits after loading cannot change the run.

Setup and reset execute through psql, each in one transaction with stop-on-error
and a 30-second timeout. Their timings and redacted diagnostics are separate
from warm-up and measured traffic. Use plain SQL without psql commands, file
includes, transaction control, or operations that cannot run in a transaction.
pgbench variables such as `:customer_count` are available only to workload SQL.
Qualify table names in every script; `schema` does not set the search path.

Gengo executes the reset you supply. It does not take database snapshots or
infer which rows to restore. Reset must restore every value, row, and sequence
that matters to your experiment. Scripts are trusted code. Keep the experiment
in tables it owns and avoid overlapping runs against shared mutable state.

The example leaves its schema and table in place with balances restored after
a successful reset-policy run. Schema removal is a separate operator action.

## Inspect and compare results

The output directory retains `workload.sql`, optional `setup.sql` and `reset.sql`,
and per-lifecycle directories containing `psql.txt`. `result.json` records:

- `starting_state`, matching the effective scenario policy.
- `workload_sha256`, `setup_sha256`, and `reset_sha256` for the scripts used.
- Target `setup` status, timing, and diagnostics.
- Step `reset_before_warmup`, `reset_before_measurement`, and `reset_after_step`
  status, timing, and diagnostics where applicable.

Offline comparisons require matching access mode, state policy, and lifecycle
script hashes, in addition to existing workload and measurement settings. Paths
may differ. Failed or missing setup/reset phases cannot be compared as complete
experiments. Comparisons of `reuse` runs warn that starting data may differ even
when all scripts match.

## Verify the lifecycle

```powershell
$env:GENGO_POSTGRES_TEST_ENV_FILE = (Resolve-Path .env).Path
$env:GENGO_POSTGRES_TEST_TARGETS = 'direct, shared, dedicated'
go test ./tests -run '^TestWriteResetLive$' -count=1 -v
```

The opt-in test uses an isolated schema and database audit rows to check phase
boundaries. It compares committed write counts with measured completed iterations,
verifies a zero baseline after each reset step, and checks that reuse retains
warm-up and measured writes. It also checks transaction rollback for failing
setup, reset, and workload scripts. Tests remove their isolated schema afterward.

On 22 November 2026, live CLI write/reset sweeps passed at one and two clients
across direct, shared Supavisor, and dedicated PgBouncer connections. All six
measured steps matched database audit counts and left the baseline restored.
Direct-endpoint reuse and setup/reset/workload rollback checks also passed.
Process-fixture tests cover exact lifecycle order, each reset failure point,
source-file snapshotting, cancellation, and comparison compatibility. Build,
vet, and the full Windows suite passed with `GENGO_TEST_SIZE_GB=0.00001` for
generation fixtures. These short runs verify behavior, not endpoint capacity.

Built-in write presets remain a separate increment. Custom scripts can already
exercise inserts, updates, deletes, and mixed reads/writes under these policies.
