# Gengo verification map

Read this index before driving Gengo. Use a fresh helper run for each verification;
the helper prints its evidence directory. The standard preconditions are Go 1.25+,
the repository root, `psql` and `pgbench` on PATH, and a dotenv file with a direct
connection plus any requested pooler targets. Credentials are never fixtures.

The default helper covers the five features below through real commands and
independent observations. `-offline` covers only generation and import planning.
The automated smoke runs tiny workloads; it does not establish production capacity.
Existing deeper live tests are opt-in and have their own isolated-schema cleanup.
Use absolute `GENGO_POSTGRES_TEST_ENV_FILE` paths for them and unset the variable
afterward. Configure `GENGO_POSTGRES_TEST_TARGETS=direct,shared,dedicated` when
pooler coverage is intended; the test default is direct only.

- [Generation](generation.md): all models/formats, seeded runs, dry-run, publication.
- [Connections](connections.md): direct and pooled read-only connectivity.
- [Imports](imports.md): planning, execution, constraints, rejection, rollback.
- [Workloads](workloads.md): lookup/custom SQL, state policies, load and connection controls.
- [Comparisons](comparisons.md): compatible saved experiments and rejection paths.

Capture the exact command, output, exit status, and resulting files or SQL state.
Record skipped entry points explicitly. An offline test, doctor, or one protocol
must not be reported as verification of the other paths. Evidence survives cleanup.
