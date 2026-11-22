package benchmark

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func init() {
	mode := os.Getenv("GENGO_BENCH_HELPER")
	if mode == "" {
		return
	}
	arguments := strings.Join(os.Args[1:], "\n")
	if strings.Contains(arguments, "--version") {
		if os.Args[1] != "--version" {
			os.Exit(2)
		}
		fmt.Println("pgbench (PostgreSQL) 17.12")
		os.Exit(0)
	}
	if strings.Contains(arguments, "--set=ON_ERROR_STOP=on") {
		if mode == "preflight-error" {
			fmt.Println("17.12|2|1|3|2")
		} else {
			fmt.Println("17.12|3|1|3|3")
		}
		os.Exit(0)
	}
	for _, required := range []string{"--no-vacuum", "--exit-on-abort", "--protocol=simple", "--max-tries=1", "--random-seed=0", "--define=customer_count=3"} {
		if !strings.Contains(arguments, required) {
			os.Exit(2)
		}
	}
	for _, argument := range os.Args[1:] {
		if prefix, found := strings.CutPrefix(argument, "--log-prefix="); found {
			if mode == "execution-error" {
				fmt.Fprintln(os.Stderr, "SQL error "+os.Getenv("PGPASSWORD"))
				os.Exit(2)
			}
			contents := "0 1 1000 0 1700000000 0\n0 2 2000 0 1700000000 0\n0 3 3000 0 1700000000 0\n0 4 9000 0 1700000000 0\n0 5 failed 0 1700000000 0\n"
			if err := os.WriteFile(prefix+".1", []byte(contents), 0600); err != nil {
				os.Exit(2)
			}
		}
	}
	fmt.Print(measurementSummary)
	os.Exit(0)
}

func installClientHelpers(t *testing.T) {
	t.Helper()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	directory := t.TempDir()
	for _, name := range []string{"psql", "pgbench"} {
		if runtime.GOOS == "windows" {
			name += ".exe"
		}
		path := filepath.Join(directory, name)
		if err := os.Link(executable, path); err == nil {
			continue
		}
		source, err := os.Open(executable)
		if err != nil {
			t.Fatal(err)
		}
		destination, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0755)
		if err != nil {
			source.Close()
			t.Fatal(err)
		}
		_, copyErr := io.Copy(destination, source)
		source.Close()
		closeErr := destination.Close()
		if copyErr != nil || closeErr != nil {
			t.Fatalf("install client helper: %v, %v", copyErr, closeErr)
		}
	}
	t.Setenv("PATH", directory+string(os.PathListSeparator)+os.Getenv("PATH"))
	for _, variable := range []string{"GENGO_DIRECT_DATABASE_URL", "GENGO_TRANSACTION_DATABASE_URL", "GENGO_DEDICATED_DATABASE_URL"} {
		t.Setenv(variable, "postgresql://postgres@invalid.example/postgres")
	}
	t.Setenv("PGPASSWORD", "private-helper-password")
}

func TestRunSweepsContinueAndSaveMeasurements(t *testing.T) {
	installClientHelpers(t)
	t.Setenv("GENGO_BENCH_HELPER", "success")
	for _, passing := range []bool{false, true} {
		contents := validScenario
		if passing {
			contents = strings.ReplaceAll(contents, "max_error_rate: 0.01", "max_error_rate: 0.2")
		}
		plan, err := LoadScenario(scenarioFixture(t, contents))
		if err != nil {
			t.Fatal(err)
		}
		output := filepath.Join(t.TempDir(), "results")
		var progress bytes.Buffer
		result, err := Run(t.Context(), plan, "", output, &progress)
		if (err == nil) != passing || result == nil || len(result.Steps) != 6 {
			t.Fatalf("incomplete sweep, passing=%t: %+v, %v", passing, result, err)
		}
		for _, step := range result.Steps {
			if step.Warmup.Status != "completed" || step.Measurement.Status != "completed" || step.Measurement.Metrics.P95MS != 9 || step.Measurement.Metrics.CompletedIterations != 4 {
				t.Fatalf("incorrect phase results: %+v", step)
			}
			if passing && (step.Status != "passed" || result.Capacity[step.Target] != 2) {
				t.Fatalf("incorrect passing capacity: %+v", result)
			}
			if !passing && (step.Status != "failed" || len(step.Failures) != 1 || result.Capacity[step.Target] != 0) {
				t.Fatalf("threshold failure did not continue sweep: %+v", result)
			}
		}
		encoded, err := os.ReadFile(filepath.Join(output, "result.json"))
		if err != nil || bytes.Contains(encoded, []byte("private-helper-password")) {
			t.Fatalf("missing or unsafe result: %v", err)
		}
		var saved Result
		if err := json.Unmarshal(encoded, &saved); err != nil || saved.FinishedAt == nil || len(saved.Steps) != 6 {
			t.Fatalf("incomplete saved experiment: %v", err)
		}
	}
}

func TestRunPreservesPreflightAndExecutionErrors(t *testing.T) {
	installClientHelpers(t)
	for _, mode := range []string{"preflight-error", "execution-error"} {
		t.Run(mode, func(t *testing.T) {
			t.Setenv("GENGO_BENCH_HELPER", mode)
			plan, err := LoadScenario(scenarioFixture(t, validScenario))
			if err != nil {
				t.Fatal(err)
			}
			output := filepath.Join(t.TempDir(), "results")
			var progress bytes.Buffer
			result, err := Run(t.Context(), plan, "", output, &progress)
			if err == nil || result == nil || result.Status != "failed" {
				t.Fatalf("missing saved failure: %+v, %v", result, err)
			}
			if mode == "preflight-error" && (len(result.Steps) != 0 || result.Targets["dedicated"].Error == "") {
				t.Fatalf("preflight errors were not preserved: %+v", result)
			}
			if mode == "execution-error" && (len(result.Steps) != 6 || result.Steps[5].Status != "error" || !strings.Contains(result.Steps[5].Measurement.Error, "[REDACTED]")) {
				t.Fatalf("execution failure did not continue or redact: %+v", result)
			}
			encoded, err := os.ReadFile(filepath.Join(output, "result.json"))
			if err != nil || bytes.Contains(encoded, []byte("private-helper-password")) {
				t.Fatalf("missing or unsafe failure result: %v", err)
			}
		})
	}
}

func TestCanceledRunSavesResultAndRejectsOverwrite(t *testing.T) {
	t.Setenv("GENGO_DIRECT_DATABASE_URL", "postgresql://postgres:private-password@invalid.example/postgres")
	path := scenarioFixture(t, validScenario)
	plan, err := LoadScenario(path)
	if err != nil {
		t.Fatal(err)
	}
	plan.Scenario.Targets = []string{"direct"}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	output := filepath.Join(t.TempDir(), "results")
	var progress bytes.Buffer
	result, err := Run(ctx, plan, "", output, &progress)
	if err == nil || result == nil || result.Status != "canceled" || result.FinishedAt == nil {
		t.Fatalf("expected persisted cancellation, got %+v, %v", result, err)
	}
	contents, err := os.ReadFile(filepath.Join(output, "result.json"))
	if err != nil {
		t.Fatal(err)
	}
	var saved Result
	if err := json.Unmarshal(contents, &saved); err != nil {
		t.Fatal(err)
	}
	if saved.Status != "canceled" || saved.DatasetID != plan.Dataset.DatasetID || bytes.Contains(contents, []byte("private-password")) || bytes.Contains(contents, []byte("invalid.example")) {
		t.Fatalf("invalid saved cancellation: %s", contents)
	}
	if _, err := Run(ctx, plan, "", output, &progress); err == nil {
		t.Fatal("overwrote an existing result directory")
	}
	after, err := os.ReadFile(filepath.Join(output, "result.json"))
	if err != nil || !bytes.Equal(after, contents) {
		t.Fatal("overwrite rejection damaged previous result")
	}
}
