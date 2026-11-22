---
name: verify-gengo
description: Build, test, and drive Gengo's real CLI through generation, PostgreSQL imports, benchmarks, saved comparisons, and cleanup. Use after generation or workload changes and for an end-to-end smoke check against configured database targets.
---

# Verify Gengo

Read [the feature map](features/README.md) before choosing coverage. The primary
surface is the short-lived `gengo` CLI. There is no server or browser to launch.
Run from the repository root. Use Go 1.25 or later and the pinned `go.mod`.

## Launch

Put `go`, `psql`, and `pgbench` on `PATH`. If Go is missing on Windows, inspect
the local, ignored `dev.ps1` to locate its portable toolchain, then put that
toolchain's `bin` directory on `PATH` and use its GOPATH/GOCACHE settings.
Do not commit machine-specific paths or install a different toolchain silently.

Run the standard smoke check:

```sh
go run ./.agents/skills/verify-gengo/scripts -env-file .env
```

It runs uncached `go test ./...`, `go vet ./...`, and `go build`, then drives the
built binary. It uses small E2E fixtures through `GENGO_TEST_SIZE_GB=0.0001` and
disables the separate opt-in live tests during the initial test pass. The helper
then performs its own explicit live CLI checks. Success means exit code zero,
`summary.json` status `passed`, database `verified`, and `cleanup_verified: true`.
Read the summary rather than treating a command's `PASS` line as the whole proof.

The tracked `.githooks/pre-commit` hook runs the all-endpoint smoke check before
commits. Enable it per clone with `git config --local core.hooksPath .githooks`.
Portable Go installations can use local `gengo.goExecutable`, `gengo.goPath`, and
`gengo.goCache` Git settings. Failed verification blocks the commit. The hook
checks the working tree; it does not create an isolated copy of staged files.

To cover every configured endpoint:

```sh
go run ./.agents/skills/verify-gengo/scripts -env-file .env -targets direct,shared,dedicated
```

Credentials come from Gengo's existing dotenv and process-environment handling.
Process variables take precedence. Pass paths, never passwords or connection
URLs, on command lines. Do not print or commit `.env`.

For Supabase telemetry, add `-require-metrics`. This requires `SUPABASE_API_KEY`,
checks snapshot metadata and file checksums for every experiment, and adds a
65-second read-only direct experiment at one requested iteration per second.
Require at least two snapshots in `metrics/result.json` to prove periodic
collection. This mode cannot be combined with `-offline` or `-doctor`.
Require `metrics_verified: true` in `summary.json` for this mode. The helper also
checks that the API key and its Basic authentication encoding are absent from
JSON, logs, and Prometheus snapshots in the evidence directory.

Each run prints `Evidence:` followed by a unique directory in the OS temporary
directory. An optional `-evidence` path must be a new directory with an existing
parent. The helper builds one private binary and deletes it during teardown.
There is no persistent process to stop after success.

## Doctor

When anything looks off, run this read-only database check first:

```sh
go run ./.agents/skills/verify-gengo/scripts -doctor -env-file .env -targets direct,shared,dedicated
```

It builds the current checkout, checks CLI help and client versions, and runs
`gengo db check` for each requested target plus `SELECT 1` on direct. It creates
local evidence but no database schema. Missing tools, credentials, or unreachable
targets fail the check. Doctor success is not generation or benchmark coverage.

## Drive

The helper's default target is `direct`; import and database audit always use
direct, even if the benchmark target list contains only poolers.

The live path drives five mapped features:

1. [Generation](features/generation.md): dry-run without output, seeded CSV
   generation, manifest configuration, and observed per-file/total CSV counts.
2. [Connections](features/connections.md): real read-only checks for requested
   targets using the current binary.
3. [Imports](features/imports.md): plan and execute import into a fresh schema,
   then observe primary keys, validated foreign keys, and joined rows.
4. [Workloads](features/workloads.md): two repeated lookup experiments with
   rotating target order, a prepared-protocol connection burst, and an extended
   protocol arrival-rate write/reset experiment. Database audit rows prove warm-up
   writes were reset and measured writes match saved iterations. The counter must
   finish at zero.
5. [Comparisons](features/comparisons.md): compare two independently saved lookup
   results and check the saved comparison's dataset identity and target coverage.

The smallest local check, with database coverage explicitly skipped, is:

```sh
go run ./.agents/skills/verify-gengo/scripts -offline
```

Offline mode still tests, vets, builds, generates, checks CSV counts, and creates
an import plan. Its summary says database `skipped`; never report it as live E2E.

## Evidence

Keep the printed evidence directory. It contains `summary.json`, one `.log` and
`.command.json` per process, the dataset/manifest, generated import scripts,
scenario and SQL snapshots, four benchmark result directories, `comparison.json`,
database audit SQL/output, and cleanup SQL/output. Commands record exit codes;
database query errors also fail the helper. Failed attempts retain their evidence.

Proof must exercise the actual CLI and observe files and database effects.
The helper only uses the PostgreSQL client API for doctor, independent SQL audit,
and teardown. It does not call generation or benchmark execution APIs to replace
the user path. No external service is mocked. Dry-run proof checks that its
requested output directory is absent. This does not establish absence of every
possible network effect; the inspected generation path has no database calls.

The initial test log covers all four models and both formats plus the existing
offline scenario tests. The short live smoke is not the exhaustive live matrix
or a performance/capacity result. Follow the feature map for overload, reconnect,
reuse, rollback, protocol-matrix, and threshold-failure coverage.

## Cleanup

The helper creates a random `gengo_verify_` schema, requires it to be absent before
import, and drops only that schema in a deferred cleanup with a fresh timeout.
It queries `pg_namespace` afterward and requires zero matching schemas. Concurrent
runs use distinct schemas, directories, and binaries, though their load on the
same database can affect timings. No settings or existing application rows change.

Cleanup runs after success and normal errors. Never kill processes by name. A
hard kill, machine shutdown, or database outage can prevent cleanup. In that case,
read the failed run's `summary.json` and `cleanup-drop.sql`, confirm the exact
schema belongs to that run, and execute that retained SQL through the existing
credential-safe PostgreSQL client. Do not substitute a broad schema-name search.
Verify absence afterward. Never delete the evidence directory as cleanup.

After a run, open `summary.json`, `cleanup-check.log`, and a result or audit file.
Confirm these files still exist and that the temporary binary has been removed.

## Helpers

`scripts/main.go` is the executable helper. Invoke it with the `go run` commands
above. `go run ./.agents/skills/verify-gengo/scripts -help` lists its flags. It
requires the repository root as working directory and supports Windows and Unix
paths. No additional package installation is needed.

Keep the map current with `/maintain-verification-skill` when CLI behavior changes.
