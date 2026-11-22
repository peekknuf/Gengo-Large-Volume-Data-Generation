# Generate datasets

Generate reproducible CSV or Parquet data from ecommerce, ecommerce-ds, financial,
or medical row plans, then inspect the completed manifest and files.

## Sub-features

- `gen-plan`: dry-run and explicit row overrides.
- `gen-formats`: all four models in CSV and Parquet.
- `gen-repeat`: seeded output across worker counts and file boundaries.
- `gen-publication`: complete manifests and rejection of occupied destinations.

## How to get to it (user POV)

Run `gengo gen`; flags or interactive prompts select model, size, format, and output.
`gengo gen --help` lists seed, reference-date, row, worker, and shard controls.

## Driving it with verify-gengo

Preconditions: repository root and Go on PATH. No database is needed.

- Run `go run ./.agents/skills/verify-gengo/scripts -offline`. The helper runs the
  eight model/format E2E cases, then explicitly generates seven orders and 39 items
  with seed zero, date 2026-11-21, two workers, and four orders per shard.
- Inspect `dry-run.command.json` and `dry-run.log`; the helper requires that the
  requested dataset directory does not exist before the real generation command.
- Inspect `generate.command.json`, `generation-proof.json`, and dataset CSVs.
  Actual CSV row totals must agree with every manifest file and table count.
- For a focused reproducibility check run
  `go test ./internal/core -run 'TestReproducibleRuns|TestFinancialFileBoundariesPreserveSeries|TestPublishCompletedDataset|TestZeroCountRuns' -count=1`.
  These check worker independence, CSV/Parquet logical values, retained financial
  state, empty tables, and publication failures.

## Gotchas

- `-short` does not skip `TestE2E`. The helper explicitly selects small fixtures.
- The helper's explicit retained dataset is ecommerce CSV. Other model/format
  proof is in `go-test.log`; temporary fixtures from those tests are cleaned up.
- Equal size estimates can produce different row plans across formats. Compare
  explicit counts, not requested sizes. Timing fields are not dataset identity.
- Interactive prompt entry is mapped but not driven by the noninteractive helper.
