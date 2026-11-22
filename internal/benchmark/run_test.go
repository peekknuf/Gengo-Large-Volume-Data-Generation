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
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/peekknuf/Gengo/internal/postgres"
)

func init() {
	mode := os.Getenv("GENGO_BENCH_HELPER")
	if mode == "" {
		return
	}
	arguments := strings.Join(os.Args[1:], "\n")
	stateEvent := func(event string) {
		path := os.Getenv("GENGO_STATE_EVENTS")
		file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
		if err != nil {
			os.Exit(2)
		}
		fmt.Fprintln(file, event)
		file.Close()
		contents, _ := os.ReadFile(path)
		failure := os.Getenv("GENGO_STATE_FAIL")
		if failure == event || (strings.HasPrefix(failure, "reset-") && event == "reset" && failure == fmt.Sprintf("reset-%d", strings.Count(string(contents), "reset\n"))) {
			fmt.Fprintln(os.Stderr, "state failure "+os.Getenv("PGPASSWORD"))
			os.Exit(2)
		}
		if failure == "cancel" && event == "warmup" {
			time.Sleep(time.Minute)
		}
	}
	if strings.Contains(arguments, "--version") {
		if os.Args[1] != "--version" {
			os.Exit(2)
		}
		fmt.Println("pgbench (PostgreSQL) 17.12")
		os.Exit(0)
	}
	if strings.Contains(arguments, "--set=ON_ERROR_STOP=on") {
		if mode == "state" {
			for index, argument := range os.Args[1:] {
				if argument == "--file" {
					if !strings.Contains(arguments, "--single-transaction") {
						os.Exit(2)
					}
					path := os.Args[index+2]
					stateEvent(strings.TrimSuffix(filepath.Base(path), ".sql"))
					os.Exit(0)
				}
			}
			fmt.Println("17.12")
			os.Exit(0)
		}
		if mode == "custom" || mode == "arrival" || mode == "connection" {
			if !strings.Contains(arguments, "SELECT current_setting('server_version');") {
				os.Exit(2)
			}
			fmt.Println("17.12")
			os.Exit(0)
		}
		if mode == "preflight-error" {
			fmt.Println("17.12|2|1|3|2")
		} else {
			fmt.Println("17.12|3|1|3|3")
		}
		os.Exit(0)
	}
	requiredArgs := []string{"--no-vacuum", "--exit-on-abort", "--max-tries=1", "--random-seed=0"}
	if mode == "connection" {
		requiredArgs = append(requiredArgs, "--protocol="+os.Getenv("GENGO_CONNECTION_PROTOCOL"))
	} else {
		requiredArgs = append(requiredArgs, "--protocol=simple")
	}
	if mode != "custom" && mode != "state" && mode != "arrival" && mode != "connection" {
		requiredArgs = append(requiredArgs, "--define=customer_count=3")
	}
	burstClients := 0
	if mode == "connection" {
		connection := os.Getenv("GENGO_CONNECTION_MODE")
		if strings.Contains(arguments, "--connect") != (connection != "persistent") {
			os.Exit(2)
		}
		if connection == "burst" {
			if os.Getenv("GENGO_CONNECTION_WAIT") == "1" {
				time.Sleep(time.Minute)
			}
			for _, argument := range os.Args[1:] {
				if clients, ok := strings.CutPrefix(argument, "--client="); ok {
					burstClients, _ = strconv.Atoi(clients)
				}
			}
			if !strings.Contains(arguments, "--jobs="+strconv.Itoa(burstClients)) || !strings.Contains(arguments, "--transactions=1") || strings.Contains(arguments, "--time=") {
				os.Exit(2)
			}
		} else if !strings.Contains(arguments, "--jobs=1") || !strings.Contains(arguments, "--time=2") || strings.Contains(arguments, "--transactions=") {
			os.Exit(2)
		}
	}
	for _, required := range requiredArgs {
		if !strings.Contains(arguments, required) {
			os.Exit(2)
		}
	}
	if mode == "arrival" && (!strings.Contains(arguments, "--latency-limit=1000") || (!strings.Contains(arguments, "--rate=2\n") && !strings.Contains(arguments, "--rate=100\n"))) {
		os.Exit(2)
	}
	if mode == "state" {
		if strings.Contains(arguments, "--log-prefix=") {
			stateEvent("measurement")
		} else {
			stateEvent("warmup")
		}
	}
	for _, argument := range os.Args[1:] {
		if script, found := strings.CutPrefix(argument, "--file="); found && mode == "custom" {
			contents, err := os.ReadFile(strings.TrimSuffix(script, "@1"))
			if err != nil || string(contents) != "BEGIN READ ONLY;\n\\set id random(1, 3)\nSELECT :id;\nCOMMIT;\n" {
				os.Exit(2)
			}
		}
		if prefix, found := strings.CutPrefix(argument, "--log-prefix="); found {
			if mode == "execution-error" {
				fmt.Fprintln(os.Stderr, "SQL error "+os.Getenv("PGPASSWORD"))
				os.Exit(2)
			}
			contents := "0 1 1000 0 1700000000 0\n0 2 2000 0 1700000000 0\n0 3 3000 0 1700000000 0\n0 4 9000 0 1700000000 0\n0 5 failed 0 1700000000 0\n"
			if mode == "arrival" {
				contents = rateLog
			}
			if burstClients > 0 {
				contents = ""
				for index := 0; index < burstClients; index++ {
					contents += fmt.Sprintf("%d 1 1000 0 1700000000 0\n", index)
				}
			}
			if err := os.WriteFile(prefix+".1", []byte(contents), 0600); err != nil {
				os.Exit(2)
			}
		}
	}
	if mode == "arrival" {
		fmt.Print(rateSummary)
	} else if mode == "connection" && os.Getenv("GENGO_CONNECTION_MODE") != "persistent" {
		summary := reconnectSummary
		if burstClients > 0 {
			summary = strings.ReplaceAll(summary, "processed: 4", fmt.Sprintf("processed: %d/%d", burstClients, burstClients))
			summary = strings.ReplaceAll(summary, "failed transactions: 1 (20.000%)", "failed transactions: 0 (0.000%)")
		}
		fmt.Print(summary)
	} else {
		fmt.Print(measurementSummary)
	}
	os.Exit(0)
}

func installClientHelpers(t *testing.T) string {
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
	return directory
}

func TestClientHelpersReleaseExecutableFiles(t *testing.T) {
	directory := installClientHelpers(t)
	t.Setenv("GENGO_BENCH_HELPER", "success")
	client, err := postgres.NewClient("direct", "")
	if err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	if err := client.Run(t.Context(), nil, &output, &output); err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(output.String()) != "17.12|3|1|3|3" {
		t.Fatalf("unexpected helper psql output: %s", output.String())
	}
	output.Reset()
	if err := client.RunPgbench(t.Context(), []string{"--version"}, &output, &output); err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(output.String()) != "pgbench (PostgreSQL) 17.12" {
		t.Fatalf("unexpected helper pgbench output: %s", output.String())
	}
	for _, name := range []string{"psql", "pgbench"} {
		if runtime.GOOS == "windows" {
			name += ".exe"
		}
		if err := os.Remove(filepath.Join(directory, name)); err != nil {
			t.Fatalf("helper executable remains locked after exit: %v", err)
		}
	}
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
		if _, err := ReadResult(filepath.Join(output, "result.json")); err != nil {
			t.Fatalf("runner produced a result that cannot be compared: %v", err)
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
