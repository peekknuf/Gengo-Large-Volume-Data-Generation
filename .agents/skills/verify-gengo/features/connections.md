# Check database connections

Verify database identity and PostgreSQL version through direct and pooler endpoints
without changing data.

## Sub-features

- `db-direct`: direct PostgreSQL identity and connectivity.
- `db-poolers`: shared and dedicated transaction endpoints.
- `db-libpq`: ambient libpq configuration and timeout/error handling.

## How to get to it (user POV)

Run `gengo db check --target direct --env-file .env`. Targets are direct, shared,
dedicated, and libpq. `--timeout` bounds the check.

## Driving it with verify-gengo

Preconditions: Go, psql, and pgbench on PATH, and supported credentials configured.

- Run `go run ./.agents/skills/verify-gengo/scripts -doctor -env-file .env -targets direct,shared,dedicated`.
  Require zero exit status and inspect each `doctor-*.log` for the returned server
  version and connection-check value 1. `doctor-query.sql/log` records a second
  direct query.
- Inspect `go-version.log`, `psql-version.log`, `pgbench-version.log`, and
  `go-build.command.json` to identify the tools and checkout that were driven.
- For libpq, configure its environment through an existing credential source and
  run the built CLI with `db check --target libpq --timeout 20s`; retain output and
  exit status. The smoke helper's target list intentionally covers named endpoints.

## Gotchas

- Environment values override dotenv values. Do not change stored credentials to
  work around a transient DNS problem.
- A missing pooler is a failed requested target, not a silent downgrade to direct.
- Doctor success proves connectivity only; it performs no import or benchmark.
