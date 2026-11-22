package tests

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/peekknuf/Gengo/internal/benchmark"
)

func TestArrivalRateLive(t *testing.T) {
	envFile := os.Getenv("GENGO_POSTGRES_TEST_ENV_FILE")
	if envFile == "" {
		t.Skip("set GENGO_POSTGRES_TEST_ENV_FILE to opt into arrival-rate experiments")
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
	root := t.TempDir()
	scenario := fmt.Sprintf("version: 1\nname: arrival-live\nworkload: custom-sql\nsql_file: query.sql\ntargets: [%s]\nseed: 0\nprotocol: simple\nclients: 1\narrival_rates: [2, 100]\nlatency_limit: 1s\nwarmup: 1s\nduration: 8s\nthresholds:\n  max_p95_ms: 1000000000\n  max_error_rate: 0\n  min_tps: 0\n  max_skipped_rate: 0.5\n  min_rate_attainment: 0.25\n", targets)
	for name, contents := range map[string]string{"query.sql": "SELECT pg_sleep(0.02);", "scenario.yaml": scenario} {
		if err := os.WriteFile(filepath.Join(root, name), []byte(contents), 0600); err != nil {
			t.Fatal(err)
		}
	}
	resultPath := filepath.Join(root, "result", "result.json")
	cmd := exec.CommandContext(t.Context(), bin, "bench", "run", "--scenario", filepath.Join(root, "scenario.yaml"), "--env-file", envFile, "--output", filepath.Dir(resultPath))
	cmd.Dir = t.TempDir()
	output, runErr := cmd.CombinedOutput()
	if runErr == nil || !bytes.Contains(output, []byte("highest passing tested arrival rate: 2 iterations/s")) {
		t.Fatalf("expected saved overload failure: %v\n%s", runErr, output)
	}
	result, err := benchmark.ReadResult(resultPath)
	if err != nil {
		t.Fatalf("read rate result: %v\n%s", err, output)
	}
	if result.LoadModel != "poisson-arrival-rate" || len(result.Steps) != 2*len(strings.Split(targets, ",")) || len(result.Capacity) != 0 {
		t.Fatalf("incorrect rate metadata: %+v", result)
	}
	for _, step := range result.Steps {
		metrics := step.Measurement.Metrics
		if step.Warmup == nil || step.Warmup.Status != "completed" || metrics == nil || metrics.RequestedIterations == nil || *metrics.RequestedIterations != metrics.CompletedIterations+metrics.FailedIterations+metrics.SkippedIterations || metrics.LatencyBasis != "scheduled-start-to-completion" || metrics.Timeouts != nil || metrics.ScheduleLagMeanMS == nil || metrics.ScheduleLagMaxMS == nil {
			t.Fatalf("missing scheduled-work measurements: %+v", step)
		}
		if step.ArrivalRate == 2 && (step.Status != "passed" || result.RateCapacity[step.Target] != 2) {
			t.Fatalf("low rate did not pass: %+v metrics=%+v", step, metrics)
		}
		if step.ArrivalRate == 100 && (step.Status != "failed" || metrics.SkippedIterations < 1 || *metrics.SkippedRate <= 0.5 || *metrics.RateAttainment >= 0.25) {
			t.Fatalf("overload did not expose skipped work and low attainment: %+v metrics=%+v", step, metrics)
		}
	}
	compare := exec.CommandContext(t.Context(), bin, "bench", "compare", resultPath, resultPath)
	compare.Dir = t.TempDir()
	if output, err := compare.CombinedOutput(); err != nil || !bytes.Contains(output, []byte("highest passing tested arrival rate: 2 baseline, 2 candidate")) {
		t.Fatalf("offline rate comparison: %v\n%s", err, output)
	}
}
