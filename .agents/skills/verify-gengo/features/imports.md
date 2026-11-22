# Import an ecommerce CSV dataset

Plan and execute an isolated PostgreSQL import, validate relational constraints,
and retain the scripts for inspection.

## Sub-features

- `load-plan`: offline manifest validation and psql scripts.
- `load-run`: copy rows, indexes, foreign keys, counts, and statistics.
- `load-errors`: existing-schema rejection and failed-copy rollback.

## How to get to it (user POV)

Run `gengo load plan` or `gengo load run` with dataset, schema, and output flags.
Execution supports direct and libpq targets. Poolers are for workload experiments.

## Driving it with verify-gengo

Preconditions: completed ecommerce CSV data; live execution also needs direct access.

- Run `go run ./.agents/skills/verify-gengo/scripts -env-file .env`. Inspect
  `load-plan/` for all eight scripts and `load-run.command.json` for the real import.
- Read `import-audit.sql/log`. The independent query must return `7|8|39`: seven
  primary keys, eight validated foreign keys, and all 39 items joining their order,
  customer, and product.
- Require `cleanup-check.log` to contain `0`, and `summary.json` to say cleanup was
  verified. The dataset and generated scripts must still exist after teardown.
- With `GENGO_POSTGRES_TEST_ENV_FILE` set to an absolute dotenv path, run
  `go test ./internal/core -run '^TestLivePostgresCSVLoad$' -count=1 -v` for actual
  duplicate-schema rejection and rollback on corrupted import counts.

## Gotchas

- An existing schema is never reused by the helper. Its random schema name is
  recorded in the summary before import and cleanup is limited to that name.
- Offline mode proves planning only. `load run` supports ecommerce CSV exclusively.
- Never run example setup/reset scripts against a shared application schema.
