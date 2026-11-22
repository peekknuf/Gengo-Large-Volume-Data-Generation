package tests

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/peekknuf/Gengo/internal/benchmark"
	"github.com/peekknuf/Gengo/internal/postgres"
)

func TestWriteResetLive(t *testing.T) {
	envFile := os.Getenv("GENGO_POSTGRES_TEST_ENV_FILE")
	if envFile == "" {
		t.Skip("set GENGO_POSTGRES_TEST_ENV_FILE to opt into write/reset experiments")
	}
	client, err := postgres.NewClient("direct", envFile)
	if err != nil {
		t.Fatal(err)
	}
	namespace := fmt.Sprintf("gengo_write_%d", time.Now().UnixNano())
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		var output bytes.Buffer
		if err := client.Run(ctx, []string{"--command", "DROP SCHEMA IF EXISTS " + namespace + " CASCADE;"}, &output, &output); err != nil {
			t.Errorf("cleanup: %v\n%s", err, output.String())
		}
	})
	query := func(sql string) string {
		t.Helper()
		var output, diagnostics bytes.Buffer
		if err := client.Run(t.Context(), []string{"-At", "--command", sql}, &output, &diagnostics); err != nil {
			t.Fatalf("inspect database: %v\n%s", err, diagnostics.String())
		}
		return strings.TrimSpace(output.String())
	}
	root := t.TempDir()
	write := func(name, contents string) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(root, name), []byte(contents), 0600); err != nil {
			t.Fatal(err)
		}
	}
	write("setup.sql", fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %[1]s; CREATE TABLE IF NOT EXISTS %[1]s.counter (id int PRIMARY KEY, n bigint NOT NULL); INSERT INTO %[1]s.counter VALUES (1, 7) ON CONFLICT DO NOTHING; CREATE TABLE IF NOT EXISTS %[1]s.audit (id bigint GENERATED ALWAYS AS IDENTITY, old_n bigint);", namespace))
	write("reset.sql", fmt.Sprintf("INSERT INTO %[1]s.audit(old_n) SELECT n FROM %[1]s.counter; UPDATE %[1]s.counter SET n=0;", namespace))
	write("query.sql", fmt.Sprintf("UPDATE %s.counter SET n=n+1 WHERE id=1;\nSELECT pg_sleep(0.002);", namespace))
	targets := os.Getenv("GENGO_POSTGRES_TEST_TARGETS")
	if targets == "" {
		targets = "direct"
	}
	scenario := fmt.Sprintf("version: 1\nname: write-reset\nworkload: custom-sql\nsql_file: query.sql\naccess_mode: read-write\nstate_policy: reset-per-phase\nsetup_file: setup.sql\nreset_file: reset.sql\ntargets: [%s]\nseed: 0\nprotocol: simple\nconcurrency: [1, 2]\nwarmup: 1s\nduration: 3s\nthresholds:\n  max_p95_ms: 1000000000\n  max_error_rate: 0\n  min_tps: 0\n", targets)
	if os.Getenv("GENGO_POSTGRES_TEST_LOAD_MODEL") == "arrival" {
		scenario = strings.ReplaceAll(scenario, "concurrency: [1, 2]", "clients: 2\narrival_rates: [10, 20]\nlatency_limit: 1s")
		scenario += "  min_rate_attainment: 0.01\n  max_skipped_rate: 1\n"
	}
	write("scenario.yaml", scenario)
	bin := binaryPath(t)
	build := exec.CommandContext(t.Context(), "go", "build", "-o", bin, ".")
	build.Dir = moduleRoot(t)
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, output)
	}
	cmd := exec.CommandContext(t.Context(), bin, "bench", "run", "--scenario", filepath.Join(root, "scenario.yaml"), "--env-file", envFile, "--output", filepath.Join(root, "result"))
	cmd.Dir = t.TempDir()
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("write/reset CLI: %v\n%s", err, output)
	}
	result, err := benchmark.ReadResult(filepath.Join(root, "result", "result.json"))
	if err != nil {
		t.Fatal(err)
	}
	if result.StartingState != "reset-per-phase" || result.SetupSHA256 == "" || result.ResetSHA256 == "" || len(result.Steps) != 2*len(strings.Split(targets, ",")) {
		t.Fatalf("missing state metadata: %+v", result)
	}
	rows := strings.Fields(query("SELECT old_n FROM " + namespace + ".audit ORDER BY id;"))
	if len(rows) != 3*len(result.Steps) || query("SELECT n FROM "+namespace+".counter;") != "0" {
		t.Fatalf("state was not restored between phases: audit=%v", rows)
	}
	for index, step := range result.Steps {
		before, _ := strconv.ParseInt(rows[3*index], 10, 64)
		warmup, _ := strconv.ParseInt(rows[3*index+1], 10, 64)
		measurement, _ := strconv.ParseInt(rows[3*index+2], 10, 64)
		initial := int64(0)
		if index == 0 {
			initial = 7
		}
		if before != initial || warmup < 1 || measurement != step.Measurement.Metrics.CompletedIterations || result.Targets[step.Target].Setup == nil {
			t.Fatalf("reset isolation disagrees with actual writes: step=%+v audit=%v", step, rows[3*index:3*index+3])
		}
	}
	if _, err := benchmark.Compare(result, result); err != nil {
		t.Fatalf("write result cannot be compared: %v", err)
	}

	t.Run("reuse", func(t *testing.T) {
		write("scenario.yaml", strings.ReplaceAll(strings.ReplaceAll(scenario, "state_policy: reset-per-phase", "state_policy: reuse"), "reset_file: reset.sql\n", ""))
		plan, err := benchmark.LoadScenario(filepath.Join(root, "scenario.yaml"))
		if err != nil {
			t.Fatal(err)
		}
		plan.Scenario.Targets = []string{"direct"}
		result, err := benchmark.Run(t.Context(), plan, envFile, filepath.Join(root, "reuse"), &bytes.Buffer{})
		if err != nil {
			t.Fatal(err)
		}
		n, _ := strconv.ParseInt(query("SELECT n FROM "+namespace+".counter;"), 10, 64)
		var measured int64
		for _, step := range result.Steps {
			measured += step.Measurement.Metrics.CompletedIterations
			if step.ResetBeforeWarmup != nil || step.ResetBeforeMeasurement != nil || step.ResetAfter != nil {
				t.Fatal("reuse performed a reset")
			}
		}
		if n <= measured || query("SELECT count(*) FROM "+namespace+".audit;") != strconv.Itoa(len(rows)) {
			t.Fatalf("reuse did not preserve warm-up and measurement writes: %d total, %d measured", n, measured)
		}
		if _, err := benchmark.ReadResult(filepath.Join(root, "reuse", "result.json")); err != nil {
			t.Fatal(err)
		}
	})
	for _, failing := range []string{"setup", "reset", "measurement"} {
		t.Run(failing+"_rollback", func(t *testing.T) {
			before := query("SELECT n FROM " + namespace + ".counter;")
			write("setup.sql", "SELECT 1;")
			write("reset.sql", "UPDATE "+namespace+".counter SET n=0;")
			write("query.sql", "UPDATE "+namespace+".counter SET n=n+1;")
			badSQL := "UPDATE " + namespace + ".counter SET n=999; SELECT 1/0;"
			switch failing {
			case "setup":
				write("setup.sql", badSQL)
			case "reset":
				write("reset.sql", badSQL)
			case "measurement":
				write("query.sql", badSQL)
			}
			write("scenario.yaml", strings.ReplaceAll(scenario, "warmup: 1s", "warmup: 0s"))
			plan, err := benchmark.LoadScenario(filepath.Join(root, "scenario.yaml"))
			if err != nil {
				t.Fatal(err)
			}
			plan.Scenario.Targets = []string{"direct"}
			directory := filepath.Join(root, failing+"-failure")
			result, err := benchmark.Run(t.Context(), plan, envFile, directory, &bytes.Buffer{})
			if err == nil || result == nil || result.Status != "failed" {
				t.Fatalf("missing saved failure: %+v, %v", result, err)
			}
			want := before
			switch failing {
			case "setup":
				if len(result.Steps) != 0 || !strings.Contains(result.Targets["direct"].Setup.Error, "division by zero") {
					t.Fatalf("setup failure did not stop traffic: %+v", result)
				}
			case "reset":
				if len(result.Steps) != 1 || result.Steps[0].Measurement != nil || !strings.Contains(result.Steps[0].ResetBeforeMeasurement.Error, "division by zero") {
					t.Fatalf("reset failure did not stop traffic: %+v", result)
				}
			case "measurement":
				want = "0"
				if len(result.Steps) != 2 {
					t.Fatal("workload error stopped remaining steps")
				}
				for _, step := range result.Steps {
					if step.Status != "error" || step.ResetAfter == nil || step.ResetAfter.Status != "completed" || !strings.Contains(step.Measurement.Error, "division by zero") {
						t.Fatalf("workload error did not restore state: %+v", step)
					}
				}
			}
			if got := query("SELECT n FROM " + namespace + ".counter;"); got != want {
				t.Fatalf("failed transaction left partial writes: got %s, want %s", got, want)
			}
			if _, err := benchmark.ReadResult(filepath.Join(directory, "result.json")); err == nil {
				t.Fatal("failed lifecycle result accepted for comparison")
			}
		})
	}
}
