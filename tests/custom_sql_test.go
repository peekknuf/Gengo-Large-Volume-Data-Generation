package tests

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/peekknuf/Gengo/internal/benchmark"
)

func TestCustomSQLLive(t *testing.T) {
	envFile := os.Getenv("GENGO_POSTGRES_TEST_ENV_FILE")
	if envFile == "" {
		t.Skip("set GENGO_POSTGRES_TEST_ENV_FILE to opt into live custom SQL sweeps")
	}
	targets := os.Getenv("GENGO_POSTGRES_TEST_TARGETS")
	if targets == "" {
		targets = "direct"
	}
	bin := binaryPath(t)
	build := exec.CommandContext(t.Context(), "go", "build", "-o", bin, ".")
	build.Dir = moduleRoot(t)
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, output)
	}
	run := func(args ...string) ([]byte, error) {
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
		defer cancel()
		cmd := exec.CommandContext(ctx, bin, args...)
		cmd.Dir = t.TempDir()
		return cmd.CombinedOutput()
	}
	root := t.TempDir()
	sqlPath := filepath.Join(root, "query.sql")
	scenarioPath := filepath.Join(root, "scenario.yaml")
	sql := "\\set id random(1, 100)\nSELECT :id, 1 / CASE WHEN current_setting('transaction_read_only') = 'on' THEN 1 ELSE 0 END;\nSELECT pg_sleep(0.002);\n"
	scenario := fmt.Sprintf("version: 1\nname: custom-live\nworkload: custom-sql\nsql_file: query.sql\ntargets: [%s]\nseed: 0\nprotocol: simple\nconcurrency: [1, 2]\nwarmup: 1s\nduration: 5s\nthresholds:\n  max_p95_ms: 1000000000\n  max_error_rate: 0\n  min_tps: 0\n", targets)
	if err := os.WriteFile(scenarioPath, []byte(scenario), 0600); err != nil {
		t.Fatal(err)
	}
	var results []string
	for index := 0; index < 3; index++ {
		contents := sql
		if index == 2 {
			contents += "-- changed workload identity\n"
		}
		if err := os.WriteFile(sqlPath, []byte(contents), 0600); err != nil {
			t.Fatal(err)
		}
		directory := filepath.Join(root, fmt.Sprintf("run-%d", index))
		output, err := run("bench", "run", "--scenario", scenarioPath, "--env-file", envFile, "--output", directory)
		if err != nil {
			t.Fatalf("custom sweep %d: %v\n%s", index, err, output)
		}
		resultPath := filepath.Join(directory, "result.json")
		result, err := benchmark.ReadResult(resultPath)
		if err != nil {
			t.Fatal(err)
		}
		script, err := os.ReadFile(filepath.Join(directory, "workload.sql"))
		if err != nil || !bytes.Equal(script, []byte("BEGIN READ ONLY;\n"+contents+"\nCOMMIT;\n")) {
			t.Fatalf("incorrect executed script: %v", err)
		}
		hash := sha256.Sum256(script)
		if result.WorkloadSHA256 != hex.EncodeToString(hash[:]) || result.DatasetID != "" || len(result.Steps) != 2*len(strings.Split(targets, ",")) {
			t.Fatalf("incorrect workload metadata or sweep: %+v", result)
		}
		for _, step := range result.Steps {
			if step.Status != "passed" || step.Warmup == nil || step.Warmup.Status != "completed" || step.Measurement.Metrics.CompletedIterations < 1 || step.Measurement.Metrics.P95MS <= 0 || result.Capacity[step.Target] != 2 {
				t.Fatalf("invalid measured custom step: %+v", step)
			}
		}
		results = append(results, resultPath)
	}
	if output, err := run("bench", "compare", results[0], results[1]); err != nil || !strings.Contains(string(output), "highest passing tested clients: 2 baseline, 2 candidate") {
		t.Fatalf("same SQL comparison: %v\n%s", err, output)
	}
	if output, err := run("bench", "compare", results[0], results[2]); err == nil || !strings.Contains(string(output), "workload SQL identity") {
		t.Fatalf("changed SQL comparison: %v\n%s", err, output)
	}
}

func TestCustomSQLFailuresLive(t *testing.T) {
	envFile := os.Getenv("GENGO_POSTGRES_TEST_ENV_FILE")
	if envFile == "" {
		t.Skip("set GENGO_POSTGRES_TEST_ENV_FILE to opt into live custom SQL checks")
	}
	for _, test := range []struct{ name, sql, diagnostic string }{
		{"table_read", "SELECT count(*) FROM pg_catalog.pg_class;", ""},
		{"write", "UPDATE pg_catalog.pg_class SET relname = relname WHERE false;", "read-only transaction"},
		{"sql_error", "SELECT gengo_nonexistent_column FROM pg_catalog.pg_class;", "does not exist"},
	} {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			scenarioPath := filepath.Join(root, "scenario.yaml")
			contents := "version: 1\nname: custom-errors\nworkload: custom-sql\nsql_file: query.sql\ntargets: [direct]\nseed: 0\nprotocol: simple\nconcurrency: [1, 2]\nwarmup: 0s\nduration: 2s\nthresholds:\n  max_p95_ms: 1000000000\n  max_error_rate: 0\n  min_tps: 0\n"
			if err := os.WriteFile(scenarioPath, []byte(contents), 0600); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(root, "query.sql"), []byte(test.sql), 0600); err != nil {
				t.Fatal(err)
			}
			plan, err := benchmark.LoadScenario(scenarioPath)
			if err != nil {
				t.Fatal(err)
			}
			result, err := benchmark.Run(t.Context(), plan, envFile, filepath.Join(root, "result"), &bytes.Buffer{})
			if (err != nil) != (test.diagnostic != "") || result == nil || len(result.Steps) != 2 {
				t.Fatalf("unexpected sweep result: %+v, %v", result, err)
			}
			for _, step := range result.Steps {
				if step.Measurement == nil {
					t.Fatalf("missing measurement: %+v", step)
				}
				if test.diagnostic != "" && (step.Status != "error" || !strings.Contains(step.Measurement.Error, test.diagnostic)) {
					t.Fatalf("missing expected SQL error %q: %+v", test.diagnostic, step.Measurement)
				}
				if test.diagnostic == "" && (step.Status != "passed" || step.Measurement.Metrics.CompletedIterations < 1) {
					t.Fatalf("table query failed: %+v", step)
				}
			}
		})
	}
}
