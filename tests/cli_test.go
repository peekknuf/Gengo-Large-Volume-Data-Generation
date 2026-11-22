package tests

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/peekknuf/Gengo/internal/benchmark"
	"github.com/peekknuf/Gengo/internal/core"
	"github.com/peekknuf/Gengo/internal/postgres"
)

func TestGenerationControls(t *testing.T) {
	name := "gengo"
	if runtime.GOOS == "windows" {
		name += ".exe"
	}
	bin := filepath.Join(t.TempDir(), name)
	build := exec.CommandContext(t.Context(), "go", "build", "-o", bin, ".")
	build.Dir = moduleRoot(t)
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}
	run := func(t *testing.T, args ...string) ([]byte, error) {
		t.Helper()
		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()
		cmd := exec.CommandContext(ctx, bin, args...)
		cmd.Dir = t.TempDir()
		return cmd.CombinedOutput()
	}
	t.Run("load_plan", func(t *testing.T) {
		root := t.TempDir()
		dataset := filepath.Join(root, "dataset")
		out, err := run(t, "gen", "--model", "ecommerce", "--size", "0.000001", "--format", "csv", "--seed", "0", "--as-of", "2026-11-21", "--output", dataset)
		if err != nil {
			t.Fatalf("generate fixture: %v\n%s", err, out)
		}
		output := filepath.Join(root, "import")
		out, err = run(t, "load", "plan", "--dataset", dataset, "--schema", "gengo_cli", "--output", output)
		if err != nil {
			t.Fatalf("load plan: %v\n%s", err, out)
		}
		ddl, err := os.ReadFile(filepath.Join(output, "schema.sql"))
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(ddl), `CREATE SCHEMA "gengo_cli";`) {
			t.Fatal("load command did not use the requested schema")
		}
		for _, name := range []string{"run.psql", "load.sql", "indexes.sql", "foreign_keys.sql", "validate_foreign_keys.sql", "verify.sql", "analyze.sql"} {
			if _, err := os.Stat(filepath.Join(output, name)); err != nil {
				t.Fatal(err)
			}
		}
		if out, err := run(t, "load", "plan", "--dataset", dataset, "--output", output); err == nil {
			t.Fatalf("overwrote existing scripts: %s", out)
		}
		if out, err := run(t, "load", "plan", "--dataset", dataset); err == nil {
			t.Fatalf("accepted missing output flag: %s", out)
		}
		t.Run("bench_compare_offline", func(t *testing.T) {
			manifestBytes, err := os.ReadFile(filepath.Join(dataset, "manifest.json"))
			if err != nil {
				t.Fatal(err)
			}
			var manifest core.Manifest
			if err := json.Unmarshal(manifestBytes, &manifest); err != nil {
				t.Fatal(err)
			}
			seed := uint64(0)
			started := time.Date(2026, 11, 22, 12, 0, 0, 0, time.UTC)
			finished := started.Add(time.Minute)
			baseline := benchmark.Result{
				Version: 1, Status: "completed", StartedAt: started, FinishedAt: &finished,
				DatasetID: manifest.DatasetID, DatasetIdentity: manifest.Identity,
				WorkloadVersion: 1, PgbenchVersion: "pgbench (PostgreSQL) 17.12", LoadModel: "closed-loop-concurrency", StartingState: "read-only-reuse",
				Scenario: benchmark.Scenario{Version: 1, Name: "offline-cli-fixture", Dataset: dataset, Schema: "gengo_cli", Workload: "customer-lookup", Protocol: "simple", Seed: &seed, Targets: []string{"direct"}, Concurrency: []int{1}, Warmup: "0s", Duration: "2s", Thresholds: benchmark.Thresholds{MaxP95MS: 100}},
				Targets:  map[string]benchmark.TargetInfo{"direct": {ServerVersion: "17.12"}}, Capacity: map[string]int{"direct": 1},
				Steps: []benchmark.Step{{Target: "direct", Concurrency: 1, Status: "passed", Failures: []string{}, Measurement: &benchmark.Phase{Status: "completed", Metrics: &benchmark.Metrics{CompletedIterations: 100, TPS: 100, P95MS: 10}}}},
			}
			before, err := json.Marshal(baseline)
			if err != nil {
				t.Fatal(err)
			}
			var candidate benchmark.Result
			if err := json.Unmarshal(before, &candidate); err != nil {
				t.Fatal(err)
			}
			candidate.Steps[0].Measurement.Metrics.TPS = 125
			candidate.Steps[0].Measurement.Metrics.P95MS = 8
			after, err := json.Marshal(candidate)
			if err != nil {
				t.Fatal(err)
			}
			baselinePath := filepath.Join(root, "baseline.json")
			candidatePath := filepath.Join(root, "candidate.json")
			for path, contents := range map[string][]byte{baselinePath: before, candidatePath: after} {
				if err := os.WriteFile(path, contents, 0600); err != nil {
					t.Fatal(err)
				}
			}
			report := filepath.Join(root, "comparison.json")
			output, err := run(t, "bench", "compare", baselinePath, candidatePath, "--output", report)
			if err != nil || !strings.Contains(string(output), "+25.00") || !strings.Contains(string(output), "-20.00") || !strings.Contains(string(output), "statistical significance") {
				t.Fatalf("offline comparison: %v\n%s", err, output)
			}
			encoded, err := os.ReadFile(report)
			if err != nil {
				t.Fatal(err)
			}
			var saved benchmark.Comparison
			if err := json.Unmarshal(encoded, &saved); err != nil || saved.Version != 1 || len(saved.Steps) != 1 || saved.Steps[0].TPS.Delta != 25 || saved.Steps[0].P95MS.Delta != -2 {
				t.Fatalf("incorrect saved comparison: %v\n%s", err, encoded)
			}
			if output, err := run(t, "bench", "compare", baselinePath, candidatePath, "--output", report); err == nil {
				t.Fatalf("overwrote saved comparison: %s", output)
			}
			for path, expected := range map[string][]byte{baselinePath: before, candidatePath: after, report: encoded} {
				contents, err := os.ReadFile(path)
				if err != nil || !bytes.Equal(contents, expected) {
					t.Fatalf("comparison damaged %s: %v", path, err)
				}
			}
			incompatibleSeed := uint64(1)
			candidate.Scenario.Seed = &incompatibleSeed
			incompatible, err := json.Marshal(candidate)
			if err != nil {
				t.Fatal(err)
			}
			incompatiblePath := filepath.Join(root, "incompatible.json")
			if err := os.WriteFile(incompatiblePath, incompatible, 0600); err != nil {
				t.Fatal(err)
			}
			rejectedOutput := filepath.Join(root, "rejected.json")
			if output, err := run(t, "bench", "compare", baselinePath, incompatiblePath, "--output", rejectedOutput); err == nil || !strings.Contains(string(output), "workload seed") {
				t.Fatalf("compared incompatible workload seeds: %v\n%s", err, output)
			}
			if _, err := os.Stat(rejectedOutput); !os.IsNotExist(err) {
				t.Fatalf("rejected comparison created output: %v", err)
			}
		})
	})
	t.Run("database_validation", func(t *testing.T) {
		for _, arguments := range [][]string{
			{"db", "check", "--target", "unknown"},
			{"db", "check", "--timeout", "0s"},
			{"db", "check", "--env-file", filepath.Join(t.TempDir(), "missing")},
			{"load", "run", "--dataset", "missing", "--output", filepath.Join(t.TempDir(), "import"), "--target", "shared"},
			{"load", "run", "--dataset", "missing", "--output", filepath.Join(t.TempDir(), "import"), "--target", "dedicated"},
			{"load", "run", "--dataset", "missing", "--output", filepath.Join(t.TempDir(), "import"), "--timeout", "-1s"},
			{"load", "run", "--dataset", "missing"},
		} {
			if output, err := run(t, arguments...); err == nil {
				t.Fatalf("accepted invalid database arguments %v: %s", arguments, output)
			}
		}
	})
	t.Run("load_run_live", func(t *testing.T) {
		envFile := os.Getenv("GENGO_POSTGRES_TEST_ENV_FILE")
		if envFile == "" {
			t.Skip("set GENGO_POSTGRES_TEST_ENV_FILE to opt into live CLI imports")
		}
		client, err := postgres.NewClient("direct", envFile)
		if err != nil {
			t.Fatal(err)
		}
		namespace := fmt.Sprintf("gengo_cli_%d", time.Now().UnixNano())
		t.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			var output bytes.Buffer
			if err := client.Run(ctx, []string{"--command", `DROP SCHEMA IF EXISTS "` + namespace + `" CASCADE;`}, &output, &output); err != nil {
				t.Errorf("cleanup %s: %v\n%s", namespace, err, output.String())
			}
		})
		root := t.TempDir()
		dataset := filepath.Join(root, "dataset")
		if output, err := run(t, "gen", "--model", "ecommerce", "--size", "0.000001", "--format", "csv", "--seed", "0", "--as-of", "2026-11-21", "--output", dataset); err != nil {
			t.Fatalf("generate: %v\n%s", err, output)
		}
		if output, err := run(t, "db", "check", "--env-file", envFile); err != nil || !strings.Contains(string(output), "PostgreSQL connection verified (direct)") {
			t.Fatalf("connection check: %v\n%s", err, output)
		}
		arguments := []string{"load", "run", "--dataset", dataset, "--schema", namespace, "--output", filepath.Join(root, "import"), "--env-file", envFile}
		if output, err := run(t, arguments...); err != nil || !strings.Contains(string(output), "PostgreSQL import verified in schema "+namespace) {
			t.Fatalf("live CLI import: %v\n%s", err, output)
		}
		var output bytes.Buffer
		query := fmt.Sprintf("SELECT count(*) FROM pg_constraint WHERE connamespace = '%s'::regnamespace AND contype IN ('p', 'f') AND convalidated;", namespace)
		if err := client.Run(t.Context(), []string{"-At", "--command", query}, &output, &output); err != nil || strings.TrimSpace(output.String()) != "15" {
			t.Fatalf("expected 15 validated constraints: %v\n%s", err, output.String())
		}
		if output, err := run(t, "load", "run", "--dataset", dataset, "--schema", namespace, "--output", filepath.Join(root, "retry-import"), "--env-file", envFile); err == nil || !strings.Contains(string(output), "already exists") {
			t.Fatalf("existing schema not rejected: %v\n%s", err, output)
		}
		t.Run("pgbench_sweep", func(t *testing.T) {
			scenarioPath := filepath.Join(root, "lookup.yaml")
			scenario := fmt.Sprintf("version: 1\nname: cli-lookup\ndataset: %q\nschema: %s\ntargets: [direct]\nworkload: customer-lookup\nseed: 0\nprotocol: simple\nconcurrency: [1, 2]\nwarmup: 1s\nduration: 5s\nthresholds:\n  max_p95_ms: 1000000000\n  max_error_rate: 0\n  min_tps: 0\n", dataset, namespace)
			for _, failing := range []bool{false, true} {
				contents := scenario
				if failing {
					contents = strings.ReplaceAll(contents, "max_p95_ms: 1000000000", "max_p95_ms: 0.000001")
				}
				if err := os.WriteFile(scenarioPath, []byte(contents), 0600); err != nil {
					t.Fatal(err)
				}
				results := filepath.Join(root, fmt.Sprintf("benchmark-%t", failing))
				output, runErr := run(t, "bench", "run", "--scenario", scenarioPath, "--env-file", envFile, "--output", results)
				if (runErr != nil) != failing {
					t.Fatalf("unexpected benchmark exit, threshold breach=%t: %v\n%s", failing, runErr, output)
				}
				encoded, err := os.ReadFile(filepath.Join(results, "result.json"))
				if err != nil {
					t.Fatal(err)
				}
				var saved benchmark.Result
				if err := json.Unmarshal(encoded, &saved); err != nil {
					t.Fatal(err)
				}
				if len(saved.Steps) != 2 || saved.DatasetID == "" || saved.PgbenchVersion == "" || saved.Targets["direct"].ServerVersion == "" || saved.DatasetIdentity.Seed != 0 {
					t.Fatalf("incomplete benchmark result: %s", encoded)
				}
				for _, step := range saved.Steps {
					if step.Warmup == nil || step.Warmup.Status != "completed" || step.Measurement == nil || step.Measurement.Metrics == nil {
						t.Fatalf("missing independent phases: %+v", step)
					}
					metrics := step.Measurement.Metrics
					if metrics.CompletedIterations <= 0 || metrics.TPS <= 0 || metrics.P95MS <= 0 || metrics.RequestedIterations != nil || metrics.Timeouts != nil {
						t.Fatalf("invalid measured data: %+v", metrics)
					}
					if failing && (step.Status != "failed" || len(step.Failures) != 1) {
						t.Fatalf("threshold breach not preserved: %+v", step)
					}
				}
				if !failing && (saved.Status != "completed" || saved.Capacity["direct"] != 2) {
					t.Fatalf("passing capacity was not saved: %s", encoded)
				}
				if failing && (saved.Status != "failed" || saved.Capacity["direct"] != 0) {
					t.Fatalf("failed capacity was not saved: %s", encoded)
				}
			}
		})
	})
	t.Run("exact_rows", func(t *testing.T) {
		dir := filepath.Join(t.TempDir(), "financial")
		out, err := run(t, "gen", "--model", "financial", "--format", "csv", "--size", "0.001", "--rows", "companies=3", "--rows", "daily_stock_prices=7", "--output", dir)
		if err != nil {
			t.Fatalf("%v: %s", err, out)
		}

		files, err := filepath.Glob(filepath.Join(dir, "fact_daily_stock_prices_*.csv"))
		if err != nil {
			t.Fatal(err)
		}
		count := 0
		for _, path := range files {
			f, err := os.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			rows, err := csv.NewReader(f).ReadAll()
			f.Close()
			if err != nil {
				t.Fatal(err)
			}
			count += len(rows) - 1
		}
		if count != 7 {
			t.Fatalf("generated %d prices, want 7", count)
		}

	})
	t.Run("reject_row_overrides", func(t *testing.T) {
		for _, arg := range []string{"unknown=1", "patients=-1", "patients=0", "doctors=999999999999999999999"} {
			if out, err := run(t, "gen", "--model", "medical", "--format", "csv", "--size", "0.001", "--rows", arg, "--dry-run"); err == nil {
				t.Fatalf("accepted %s: %s", arg, out)
			}
		}
	})
	t.Run("dry_run", func(t *testing.T) {
		output := filepath.Join(t.TempDir(), "not-created")
		out, err := run(t, "gen", "--model", "medical", "--size", "0.001", "--format", "csv", "--workers", "2", "--dry-run", "--output", output)
		if err != nil {
			t.Fatalf("dry run: %v\n%s", err, out)
		}
		if !strings.Contains(string(out), "Dry run complete") {
			t.Fatalf("missing dry-run result: %s", out)
		}
		if _, err := os.Stat(output); !os.IsNotExist(err) {
			t.Fatalf("unexpected output: %v", err)
		}
	})
	t.Run("dry_run_without_output", func(t *testing.T) {
		out, err := run(t, "gen", "--model", "medical", "--size", "0.001", "--format", "csv", "--workers", "2", "--dry-run")
		if err != nil {
			t.Fatalf("dry run without output: %v\n%s", err, out)
		}
	})
	t.Run("invalid_workers", func(t *testing.T) {
		for _, count := range []string{"0", "-1"} {
			out, err := run(t, "gen", "--workers", count, "--dry-run")
			if err == nil || !strings.Contains(string(out), "workers must be at least 1") {
				t.Fatalf("workers %s: err=%v out=%s", count, err, out)
			}
		}
	})
	t.Run("stable_shards", func(t *testing.T) {
		output := t.TempDir()
		out, err := run(t, "gen", "--model", "ecommerce-ds", "--size", "0.0001", "--format", "csv", "--workers", "2", "--rows-per-file", "4", "--rows", "store_sales=7", "--rows", "catalog_sales=7", "--rows", "web_sales=7", "--seed", "0", "--as-of", "2026-11-21", "--output", output)
		if err != nil {
			t.Fatalf("generation: %v\n%s", err, out)
		}
		for _, table := range []string{"fact_store_sales", "fact_catalog_sales", "fact_web_sales"} {
			files, err := filepath.Glob(filepath.Join(output, table+"_*.csv"))
			if err != nil || len(files) != 2 {
				t.Errorf("%s: shards=%v err=%v", table, files, err)
			}
		}
	})
}
