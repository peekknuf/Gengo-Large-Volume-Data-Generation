# Codebase map for agent-assisted development

Reviewed 21 November 2026 against the working tree based on commit 933a902,
including the e-commerce import-planner increment.

## Assessment

The active generation path has useful correctness and reproducibility coverage.
Its package boundaries support incremental work. The main navigation hazard was
duplicated, stale repository guidance alongside a separate generator abstraction
that the CLI never calls. AGENTS.md now provides the shared starting point, and
CLAUDE.md points to that guide.

The project delivers generation and PostgreSQL import-script planning. The
experiment runner, scenario YAML, telemetry, result comparison, and database
reference environment are roadmap work.

## Generation flow

| Step | Implementation and responsibility |
| --- | --- |
| Command | main.go uses Cobra and resolves generation.Config, including an omitted seed and reference date. |
| Row plan | core.GetUserInput resolves the model, approximate size, format, and row overrides, and prints the resulting counts. |
| Validation | core.validateRowPlan checks supported counts and parent prerequisites before generation. |
| Run | core.GenerateRun creates a fresh Inventory and sibling staging directory, then dispatches model-specific orchestration. |
| Dimensions | Simulation functions use per-table streams and faker instances. Dimensions stay resident for relationships and sampling. |
| Facts | generation.Partitions schedules logical jobs. Medical partitions rows, e-commerce partitions orders and items, financial groups companies, and retail schedules each channel. |
| Serialization | Callers use generic writers, typed Parquet writers, and specialized direct CSV/Arrow encoders. |
| Completion | Successful writers register counts and key boundaries. core.writeManifest checks totals and physical sizes before publication. |

Dataset identity includes schema and algorithm versions, model, counts, seed,
reference date, and partition settings. Workers, format, and timings are outside
that logical identity. Retail size planning can select different counts for CSV
and Parquet, which changes identity through those counts.

Reproducibility covers worker scheduling for a fixed implementation and resolved
configuration. Stream labels, random draws, faker calls, and partition allocation
affect seeded output. Existing tests compare repeated runs of the same code. A
historical fixture would add coverage for accidental algorithm changes across
commits.

## PostgreSQL import planning

load_command.go adds load plan. core.WriteCSVLoadPlan consumes a completed
e-commerce CSV manifest and schema.ECommerce metadata.

Column order and field names come from existing structs. The explicit
order_timestamp_unix override matches the specialized CSV encoding. Primary and
foreign keys are represented in the schema metadata. That metadata currently
serves import planning, with wider consolidation available as future work.

The planner checks manifest version, model, format, counts, filenames, duplicate
entries, file status, byte sizes, and headers. It publishes eight psql scripts
through a staging directory. The scripts separate schema creation, copying and
count verification, primary-key indexes, foreign-key creation, foreign-key
validation, and statistics collection.

Tests exercise planning and CLI integration. Actual psql execution, transaction
behavior on a running server, and representative joins still need the local
PostgreSQL integration fixture.

## Similar-looking code with different reachability

| Location | Observed role |
| --- | --- |
| internal/generation | Active run configuration, streams, partition scheduling, and writer inventory. |
| internal/generators | Separate BaseGenerator and MedicalGenerator. No CLI or active simulation callers were found. It uses fixed counts and older global faker behavior. |
| internal/common | Supports the alternate generator implementation. Worker-pool behavior has regression coverage. |
| internal/formats/json.go | Component-level JSON support. CLI validation accepts CSV and Parquet. |
| internal/formats/csv_chunks.go | Chunk-writer helper with no callers found. Deferred Flush and Close errors are discarded. |
| ecommerce-ds.GenerateStoreSales | Older channel-based generator with no callers found. Active store sales use GenerateStoreSalesOptimized. |
| cmd/profile | Separate entry point that generates a fixed 10 GiB e-commerce CSV run and writes cpu.prof. |

Reachability was checked with repository searches. Repeat searches before migrating
or deleting implementations, particularly after features add new callers.

## High-value extension points

| Change | Files and checks to inspect |
| --- | --- |
| Generation option | main.go, core/input.go, generation/config.go, and CLI tests. |
| Counts or prerequisites | core/sizing.go, core/validation.go, input tests, contract fixtures, and manifests. |
| Model or relationship | Model structs, simulation, orchestration, schema metadata, contracts, and format comparisons. |
| Encoding | formats plus the domain's direct fact writers. Trace normal callers before consolidation. |
| Memory budget | generation.Partitions, dimension slices, retail buffers, order/item slices, and Parquet builders. |
| Import support | Schema metadata, core/load_plan.go, SQL types, and live integration tests. |
| Scenario and runner | New scenario and execution code following the product contract. Existing generation.Config describes dataset generation. |
| Structured CLI output | main prints a logo to stdout before command execution. Account for it when adding JSON result commands. |

## Risks before larger experiments

1. Worker budgets are per table. Retail runs three channels concurrently, each
   with the configured number of workers.
2. Retail CSV buffers are 64 MiB per active writer. Parquet builders and
   compression add allocations. Rows-per-file is separate from a process budget.
3. E-commerce retains order/item slices within each partition. An item partition
   can contain up to ten times the configured order partition size.
4. Active top-level operations create context.Background internally. Worker-group
   failure cancellation exists. Command cancellation needs a root context carried
   through generation, loading, and future execution.
5. Generic and specialized encoders can diverge in fields, numeric spelling,
   timestamp units, and failure handling. Cross-format checks cover these areas.
6. Historical seeded fixtures would strengthen compatibility testing before
   substantial generator changes.
7. Local verification is Windows-based. CI defines Linux and race checks, whose
   actual results require separate inspection.

## Verification map

| Tests | Coverage |
| --- | --- |
| core/contracts_test.go | Complete small-fixture keys, foreign keys, counts, and retail amounts for all model/format pairs. |
| core/reproducibility_test.go | Repeated seeds, workers 1/2/8, formats, customer correlations, financial boundaries, manifests, and zero counts. |
| core/output_test.go | Failed generation and destination publication. |
| core/load_plan_test.go | Real CSV fixtures, script contents, invalid inputs, schema names, and output reuse. |
| generation/config_test.go | Configuration and cancellation after partition failure. |
| Simulation-specific tests | Price invariants, sampling distribution, and shard creation failures. |
| formats/csv_test.go | CSV round trips, buffered failure handling, and a writer benchmark. |
| tests/cli_test.go | Normal CLI controls and generation-to-import-planner integration. |
| tests/e2e_test.go | Builds the CLI and exercises all eight generation model/format combinations. |

The import increment passed build, vet, and the full local suite during this
review. That verifies script preparation and the existing generator. The product
checklist tracks live database and workload execution separately.

## Efficient agent workflow

Start from the relevant CLI entry point and follow callees. Read the nearby tests
before changing a data contract. Use fixed seeds and small explicit plans for
diagnostics, then exercise the normal CLI path. Expand checks when shared
generation or serialization behavior changes.

Keep product decisions in product-direction.md, operational contracts in focused
feature docs, and navigation in AGENTS.md. Update checklist items with verified
status. Focused commits support reviewing and recovering individual increments.
