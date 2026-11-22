# Compare saved benchmark experiments

Use `bench compare` to compare two saved `result.json` files without connecting
to a database. No PostgreSQL client tools, environment file, or API key are needed
for comparison. Keep the original result files for the full measurement context.

```sh
gengo bench compare data/lookup-before/result.json data/lookup-after/result.json
gengo bench compare data/lookup-before/result.json data/lookup-after/result.json \
  --output data/lookup-comparison.json
```

The first argument is the baseline and the second is the candidate. The optional
output must be a new JSON file in an existing directory. Gengo does not modify
either input or overwrite a previous comparison. Without `--output`, comparison
only prints a terminal summary. Use the saved JSON for machine-readable output;
the CLI still prints its startup logo to stdout.

## Check compatibility

Version 1 supports read-only `customer-lookup` and read-only or read/write
`custom-sql` workloads, all three SQL protocols, and concurrency, arrival-rate,
or connection-burst load. Both inputs must
use the same load model. Arrival-rate results are also supported, with their
fixed client count, ordered rates, and latency limit checked for compatibility.
See [arrival-rate tests](arrival-rates.md) for the measurement contract. Both inputs must
contain finished, complete sweeps with successful preflights and completed
measurement phases. A finished experiment whose steps failed performance
thresholds is valid input. Running, canceled, partial, or execution-error results
are rejected rather than compared as zero-throughput measurements.

These settings must match:

- Workload kind and, for custom SQL, the SHA-256 of the executed script.
- Access mode, state policy, and setup/reset script hashes where configured.
- Dataset ID and identity metadata, including the generation seed and row plan.
- Imported schema and workload seed, including explicit seed zero.
- Target list and order, and concurrency levels.
- For arrival-rate runs, fixed client count, requested rate levels, and latency limit.
- Warm-up and measurement durations. Equivalent spellings such as `60s` and `1m` match.
- Latency, error-rate, and throughput thresholds.
- Repetition count, fixed/rotating endpoint-order policy, and repetition pass rule.

Intentional protocol changes and persistent/reconnect changes are allowed with
explicit comparison warnings. Each run reference records its connection mode and
protocol. Reconnect throughput includes connection setup; unpaced transaction
percentiles exclude each client's own setup. See
[connection experiments](connection-experiments.md) for timing details. Bursts
compare only with other bursts, using burst size rather than sustained capacity.

Scenario names, SQL file paths, and local dataset directory paths may differ.
Custom SQL results require a valid `workload_sha256`. Changed SQL is rejected
even if the filename is unchanged. Original SQL files are not needed for offline
comparison. Runs without datasets can only compare with other runs without
datasets; application data equivalence is the operator's responsibility.
Setup and required reset phases must have completed successfully. State-reuse
comparisons warn that matching scripts do not prove equivalent starting data.
Steps are paired by
target label and client count, not by their array position. PostgreSQL or pgbench
version differences produce warnings instead of rejecting the comparison, so an
intentional software upgrade can be evaluated.

Gengo checks the dataset ID against its identity metadata, verifies iteration
counts against recorded error rates, and recalculates step status and tested
capacity from the saved metrics and thresholds. This catches inconsistent result
files; it does not authenticate them or re-read their raw transaction logs.

For repeated experiments, the reader also checks sample identities, coverage,
schedule, and aggregate statistics. Comparisons use per-repetition medians and
retain distributions and passing counts. See
[repeated experiments](repeated-experiments.md). Original single-run results
without aggregate metadata remain supported.
Missing required comparison measurements are rejected, even when their numeric
type would otherwise default to zero. Unknown fields and unsupported versions
are rejected. Each input is limited to 4 MiB.

## Interpret changes

For each target/client pair, the summary shows baseline and candidate TPS and
successful-iteration p95, their percentage changes, error-rate changes in
percentage points, and both threshold statuses. Latency values are milliseconds.

All deltas are candidate minus baseline. Positive TPS means higher observed
throughput. Positive latency means higher observed latency. Percentage change is
the delta divided by the baseline, multiplied by 100. A zero baseline has no
defined percentage change, so Gengo prints `n/a` and saves JSON `null`. The same
applies if the percentage would exceed the numeric range.

Error-rate changes use percentage points to remain meaningful when the baseline
has no errors. For example, an error rate moving from 0 to 0.01 is an increase of
one percentage point. JSON also records the underlying rates and their changes.

Tested capacity compares the highest passing concurrency, or arrival rate, in the
same configured grid. It does not estimate saturation between or beyond tested levels.
If every level passes, the experiment has not established the database's limit.

The saved version-1 comparison includes shared scenario settings, dataset ID,
workload version, load model, starting-state policy, source run names and start
times, software versions, per-step changes, tested-capacity changes, and warnings.
The baseline's scenario supplies the shared settings; its name and local dataset
path need not equal the candidate's.

A valid comparison exits successfully even if the candidate is slower or has
threshold failures. This command reports differences; it does not impose a
separate performance-regression gate. Invalid inputs, incompatible settings, and
output-write failures give a nonzero exit code.

## Keep conclusions bounded

Single-run changes do not establish statistical significance or a repeatable
improvement. Target labels do not verify that endpoints, hardware, pool sizes, or
background traffic were identical. Both warnings appear in every comparison.
The current result format does not record enough environment details to enforce
those conditions automatically.

Matching configured phase durations also does not guarantee identical active
measurement intervals. Review the original results' connection setup times and
active intervals, especially for short remote runs. Repetitions, rotating target
order, and aggregation are available as explicit scenario controls. Their
descriptive summaries do not establish statistical significance. Metrics API
collection is deferred and is not required by this offline command.
