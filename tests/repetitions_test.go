package tests

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/peekknuf/Gengo/internal/benchmark"
)

func TestRepetitionsLive(t *testing.T) {
	envFile := os.Getenv("GENGO_POSTGRES_TEST_ENV_FILE")
	if envFile == "" {
		t.Skip("set GENGO_POSTGRES_TEST_ENV_FILE to opt into repeated experiments")
	}
	targets := os.Getenv("GENGO_POSTGRES_TEST_TARGETS")
	if targets == "" {
		targets = "direct"
	}
	targetNames := strings.Split(strings.ReplaceAll(targets, " ", ""), ",")
	bin := binaryPath(t)
	build := exec.CommandContext(t.Context(), "go", "build", "-o", bin, ".")
	build.Dir = moduleRoot(t)
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, output)
	}
	run := func(args ...string) ([]byte, error) {
		cmd := exec.CommandContext(t.Context(), bin, args...)
		cmd.Dir = t.TempDir()
		return cmd.CombinedOutput()
	}
	for _, failing := range []bool{false, true} {
		root := t.TempDir()
		scenario := fmt.Sprintf("version: 1\nname: repeated-burst\nworkload: custom-sql\nsql_file: query.sql\ntargets: [%s]\nseed: 0\nprotocol: simple\nconnection_mode: burst\nconcurrency: [1, 2]\nrepetitions: 3\ntarget_order: rotate\nmin_pass_rate: 1\nwarmup: 0s\nduration: 10s\nthresholds:\n  max_p95_ms: 1000000000\n  max_error_rate: 0\n  min_tps: 0\n", targets)
		if failing {
			scenario += "  max_connection_mean_ms: 0.000001\n"
		}
		for name, contents := range map[string]string{"scenario.yaml": scenario, "query.sql": "SELECT 1;"} {
			if err := os.WriteFile(filepath.Join(root, name), []byte(contents), 0600); err != nil {
				t.Fatal(err)
			}
		}
		directory := filepath.Join(root, "result")
		output, err := run("bench", "run", "--scenario", filepath.Join(root, "scenario.yaml"), "--env-file", envFile, "--output", directory)
		if (err != nil) != failing {
			t.Fatalf("incorrect repeated CLI exit, failing=%t: %v\n%s", failing, err, output)
		}
		result, err := benchmark.ReadResult(filepath.Join(directory, "result.json"))
		if err != nil {
			t.Fatal(err)
		}
		if len(result.Steps) != 6*len(targetNames) || len(result.Aggregates) != 2*len(targetNames) || len(result.Schedule) != 3 {
			t.Fatalf("missing repeated results: %+v", result)
		}
		index := 0
		for round := 0; round < 3; round++ {
			shift := round % len(targetNames)
			order := append(append([]string{}, targetNames[shift:]...), targetNames[:shift]...)
			if !reflect.DeepEqual(result.Schedule[round].Targets, order) {
				t.Fatalf("wrong planned rotation: %+v", result.Schedule)
			}
			for _, target := range order {
				for _, clients := range []int{1, 2} {
					step := result.Steps[index]
					if step.Repetition != round+1 || step.Target != target || step.Concurrency != clients {
						t.Fatalf("wrong actual execution order: %+v", step)
					}
					index++
				}
			}
		}
		for _, aggregate := range result.Aggregates {
			if aggregate.Measured != 3 || aggregate.Errors != 0 || aggregate.TPS == nil || aggregate.TPS.Min <= 0 || aggregate.P95MS == nil || (aggregate.Status == "failed") != failing {
				t.Fatalf("incorrect aggregate: %+v", aggregate)
			}
			if failing && (aggregate.Failed != 3 || result.Capacity[aggregate.Target] != 0) {
				t.Fatalf("failed repetitions qualified for capacity: %+v", aggregate)
			}
			if !failing && (aggregate.Passed != 3 || result.Capacity[aggregate.Target] != 2) {
				t.Fatalf("passing repetitions lost capacity: %+v", aggregate)
			}
		}
		report := filepath.Join(root, "comparison.json")
		output, err = run("bench", "compare", filepath.Join(directory, "result.json"), filepath.Join(directory, "result.json"), "--output", report)
		if err != nil || !bytes.Contains(output, []byte("Values compare medians across repetitions")) {
			t.Fatalf("repeated comparison: %v\n%s", err, output)
		}
		contents, err := os.ReadFile(report)
		if err != nil {
			t.Fatal(err)
		}
		var comparison benchmark.Comparison
		if err := json.Unmarshal(contents, &comparison); err != nil || len(comparison.Steps) != 2*len(targetNames) || len(comparison.Baseline.Aggregates) != 2*len(targetNames) {
			t.Fatalf("missing aggregate comparison: %+v, %v", comparison, err)
		}
	}
}
