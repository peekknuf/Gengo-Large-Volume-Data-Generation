# Compare saved experiments

Compare compatible saved results offline while rejecting mismatched configurations
and incomplete measurements.

## Sub-features

- `compare-saved`: readable summary and saved JSON comparison.
- `compare-compatibility`: workload/dataset/configuration mismatch rejection.
- `compare-repeated`: aggregated repeated samples and capacity decisions.

## How to get to it (user POV)

Run `gengo bench compare BASELINE_JSON CANDIDATE_JSON --output comparison.json`.
This command reads saved files and needs no database connection.

## Driving it with verify-gengo

Preconditions: two compatible saved experiments. The live helper creates both.

- Run `go run ./.agents/skills/verify-gengo/scripts -env-file .env`.
  Inspect `compare.command.json` and `compare.log` for the real command and status.
- Open `comparison.json` alongside `baseline/result.json` and
  `candidate/result.json`. Require the same generated dataset identity and one
  compared step per requested endpoint. These are separate executions, not two
  copies of a synthetic result.
- Run `go test ./internal/benchmark -run 'Compare' -count=1 -v` for compatibility,
  incomplete-measurement, and repeated-result rejection cases. The standard
  helper includes these tests in its initial uncached test pass.
- The initial `TestGenerationControls` test drives offline CLI comparison and
  checks a known throughput delta; its proof is in `go-test.log`.

## Gotchas

- Short repeated runs do not establish statistical significance.
- SQL and lifecycle hashes are part of compatibility. Do not edit saved results
  to force a comparison through validation.
- The helper's `-offline` mode does not create live baseline/candidate results;
  it covers comparison only through the existing offline tests.
