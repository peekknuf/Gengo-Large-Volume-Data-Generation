package benchmark

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func comparisonFixture(t *testing.T) *Result {
	t.Helper()
	plan, err := LoadScenario(scenarioFixture(t, validScenario))
	if err != nil {
		t.Fatal(err)
	}
	plan.Scenario.Targets = []string{"direct"}
	plan.Scenario.Warmup = "0s"
	start := time.Date(2026, 11, 22, 12, 0, 0, 0, time.UTC)
	finish := start.Add(time.Minute)
	return &Result{
		Version: 1, Status: "completed", StartedAt: start, FinishedAt: &finish,
		Scenario: plan.Scenario, DatasetID: plan.Dataset.DatasetID, DatasetIdentity: plan.Dataset.Identity,
		WorkloadVersion: 1, PgbenchVersion: "pgbench (PostgreSQL) 17.12", LoadModel: "closed-loop-concurrency", StartingState: "read-only-reuse",
		Targets: map[string]TargetInfo{"direct": {ServerVersion: "17.12"}}, Capacity: map[string]int{"direct": 2},
		Steps: []Step{
			{Target: "direct", Concurrency: 1, Status: "passed", Failures: []string{}, Measurement: &Phase{Status: "completed", Metrics: &Metrics{CompletedIterations: 100, TPS: 100, P95MS: 10}}},
			{Target: "direct", Concurrency: 2, Status: "passed", Failures: []string{}, Measurement: &Phase{Status: "completed", Metrics: &Metrics{CompletedIterations: 100, TPS: 200, P95MS: 20}}},
		},
	}
}

func saveComparisonFixture(t *testing.T, result *Result) string {
	t.Helper()
	contents, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "result.json")
	if err := os.WriteFile(path, contents, 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestCompareSavedResults(t *testing.T) {
	baseline := comparisonFixture(t)
	candidate := comparisonFixture(t)
	candidate.Scenario.Name = "candidate"
	candidate.Scenario.Duration = "2000ms"
	candidate.Scenario.Warmup = "0m"
	candidate.PgbenchVersion = "pgbench (PostgreSQL) 19.0"
	candidate.Targets["direct"] = TargetInfo{ServerVersion: "18.1"}
	candidate.Steps[0].Measurement.Metrics = &Metrics{CompletedIterations: 99, FailedIterations: 1, ErrorRate: 0.01, TPS: 125, P95MS: 5}
	candidate.Steps[1].Measurement.Metrics.TPS = 150
	candidate.Steps[1].Measurement.Metrics.P95MS = 200
	candidate.Steps[1].Status = "failed"
	candidate.Steps[1].Failures = []string{"p95 latency exceeds max_p95_ms"}
	candidate.Status = "failed"
	candidate.Capacity["direct"] = 1
	candidate.Steps[0], candidate.Steps[1] = candidate.Steps[1], candidate.Steps[0]
	loadedBaseline, err := ReadResult(saveComparisonFixture(t, baseline))
	if err != nil {
		t.Fatal(err)
	}
	loadedCandidate, err := ReadResult(saveComparisonFixture(t, candidate))
	if err != nil {
		t.Fatal(err)
	}
	comparison, err := Compare(loadedBaseline, loadedCandidate)
	if err != nil {
		t.Fatal(err)
	}
	if len(comparison.Steps) != 2 || comparison.Steps[0].Concurrency != 1 || comparison.Steps[1].Concurrency != 2 {
		t.Fatalf("steps were not matched by target and concurrency: %+v", comparison.Steps)
	}
	first, second := comparison.Steps[0], comparison.Steps[1]
	if first.TPS.Baseline != 100 || first.TPS.Candidate != 125 || first.TPS.Delta != 25 || *first.TPS.Percent != 25 || *first.P95MS.Percent != -50 || first.ErrorRatePercentagePoints != 1 || first.ErrorRate.Percent != nil {
		t.Fatalf("incorrect first-level changes: %+v", first)
	}
	if *second.TPS.Percent != -25 || second.P95MS.Delta != 180 || *second.P95MS.Percent != 900 || second.BaselineStatus != "passed" || second.CandidateStatus != "failed" {
		t.Fatalf("incorrect second-level changes: %+v", second)
	}
	if len(comparison.Capacity) != 1 || comparison.Capacity[0].Baseline != 2 || comparison.Capacity[0].Candidate != 1 || comparison.Capacity[0].Delta != -1 || len(comparison.Warnings) != 4 {
		t.Fatalf("incorrect capacity or version warnings: %+v", comparison)
	}
	var summary bytes.Buffer
	PrintComparison(&summary, comparison)
	for _, expected := range []string{"+25.00", "-50.00", "passed/failed", "percentage points", "statistical significance"} {
		if !strings.Contains(summary.String(), expected) {
			t.Fatalf("summary missing %q: %s", expected, summary.String())
		}
	}
	output := filepath.Join(t.TempDir(), "comparison.json")
	if err := WriteComparison(output, comparison); err != nil {
		t.Fatal(err)
	}
	contents, err := os.ReadFile(output)
	if err != nil {
		t.Fatal(err)
	}
	var saved Comparison
	if err := json.Unmarshal(contents, &saved); err != nil || saved.Version != 1 || saved.Steps[0].TPS.Delta != 25 || saved.Steps[0].ErrorRate.Percent != nil {
		t.Fatalf("incorrect saved comparison: %v\n%s", err, contents)
	}
	if err := WriteComparison(output, comparison); err == nil {
		t.Fatal("overwrote existing comparison")
	}
	after, err := os.ReadFile(output)
	if err != nil || !bytes.Equal(contents, after) {
		t.Fatal("overwrite rejection damaged saved comparison")
	}
}

func TestCompareRejectsIncompatibleExperiments(t *testing.T) {
	for _, testCase := range []struct {
		name   string
		mutate func(*Result)
	}{
		{"dataset", func(result *Result) {
			result.DatasetIdentity.Seed++
			identity, err := json.Marshal(result.DatasetIdentity)
			if err != nil {
				t.Fatal(err)
			}
			hash := sha256.Sum256(identity)
			result.DatasetID = hex.EncodeToString(hash[:])
		}},
		{"seed", func(result *Result) { seed := uint64(1); result.Scenario.Seed = &seed }},
		{"schema", func(result *Result) { result.Scenario.Schema = "other_schema" }},
		{"warmup", func(result *Result) {
			result.Scenario.Warmup = "1s"
			for index := range result.Steps {
				result.Steps[index].Warmup = &Phase{Status: "completed"}
			}
		}},
		{"duration", func(result *Result) { result.Scenario.Duration = "3s" }},
		{"thresholds", func(result *Result) { result.Scenario.Thresholds.MaxP95MS = 200 }},
		{"workload_version", func(result *Result) { result.WorkloadVersion = 2 }},
		{"protocol", func(result *Result) { result.Scenario.Protocol = "unknown" }},
		{"load_model", func(result *Result) { result.LoadModel = "arrival-rate" }},
		{"starting_state", func(result *Result) { result.StartingState = "reset" }},
		{"concurrency", func(result *Result) {
			result.Scenario.Concurrency = []int{1, 4}
			result.Steps[1].Concurrency = 4
			result.Capacity["direct"] = 4
		}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			baseline, candidate := comparisonFixture(t), comparisonFixture(t)
			testCase.mutate(candidate)
			if _, err := Compare(baseline, candidate); err == nil {
				t.Fatal("compared incompatible experiments")
			}
		})
	}
}

func TestReadResultRejectsIncompleteOrInconsistentMeasurements(t *testing.T) {
	for _, testCase := range []struct {
		name   string
		mutate func(*Result)
	}{
		{"running", func(result *Result) { result.Status = "running" }},
		{"canceled", func(result *Result) { result.Status = "canceled" }},
		{"unfinished", func(result *Result) { result.FinishedAt = nil }},
		{"preflight", func(result *Result) { result.Targets["direct"] = TargetInfo{Error: "failed"} }},
		{"missing_step", func(result *Result) { result.Steps = result.Steps[:1] }},
		{"duplicate_step", func(result *Result) { result.Steps[1] = result.Steps[0] }},
		{"missing_metrics", func(result *Result) { result.Steps[0].Measurement.Metrics = nil }},
		{"execution_error", func(result *Result) { result.Steps[0].Measurement.Status = "error" }},
		{"invalid_capacity", func(result *Result) { result.Capacity["direct"] = 99 }},
		{"missing_seed", func(result *Result) { result.Scenario.Seed = nil }},
		{"negative_latency", func(result *Result) { result.Steps[0].Measurement.Metrics.P95MS = -1 }},
		{"zero_tps", func(result *Result) { result.Steps[0].Measurement.Metrics.TPS = 0 }},
		{"invented_errors", func(result *Result) { result.Steps[0].Measurement.Metrics.ErrorRate = 0.5 }},
		{"forged_status", func(result *Result) { result.Steps[0].Status = "failed" }},
		{"forged_identity", func(result *Result) { result.DatasetIdentity.Seed++ }},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			result := comparisonFixture(t)
			testCase.mutate(result)
			if _, err := ReadResult(saveComparisonFixture(t, result)); err == nil {
				t.Fatal("accepted incomplete or inconsistent saved result")
			}
		})
	}
}

func TestReadResultRejectsMalformedJSON(t *testing.T) {
	valid, err := json.Marshal(comparisonFixture(t))
	if err != nil {
		t.Fatal(err)
	}
	for _, contents := range []string{
		`{"password":"do-not-echo-this"}`,
		string(valid) + "\n{}",
		strings.Replace(string(valid), `"successful_iteration_p95_ms":10`, `"removed":10`, 1),
		strings.Replace(string(valid), `"successful_iteration_p95_ms":10,`, "", 1),
		strings.Replace(string(valid), `"successful_iteration_p95_ms":10`, `"successful_iteration_p95_ms":null`, 1),
	} {
		path := filepath.Join(t.TempDir(), "result.json")
		if err := os.WriteFile(path, []byte(contents), 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := ReadResult(path); err == nil || strings.Contains(err.Error(), "do-not-echo-this") {
			t.Fatalf("expected safe JSON rejection, got %v", err)
		}
	}
}

func TestMeasuredChangeHandlesZeroAndOverflow(t *testing.T) {
	zero := measuredChange(0, 0)
	fromZero := measuredChange(0, 5)
	if zero.Delta != 0 || zero.Percent != nil || fromZero.Delta != 5 || fromZero.Percent != nil {
		t.Fatalf("zero baseline invented a percentage: %+v, %+v", zero, fromZero)
	}
	if change := measuredChange(1e-300, 1e300); change.Percent != nil || change.Delta != 1e300 {
		t.Fatalf("overflow did not produce an unavailable percentage: %+v", change)
	}
}

func TestCompareRejectsDifferentTargetOrder(t *testing.T) {
	baseline, candidate := comparisonFixture(t), comparisonFixture(t)
	for _, result := range []*Result{baseline, candidate} {
		result.Scenario.Targets = []string{"direct", "shared"}
		result.Targets["shared"] = TargetInfo{ServerVersion: "17.12"}
		result.Capacity["shared"] = 2
		for _, step := range result.Steps[:2] {
			step.Target = "shared"
			result.Steps = append(result.Steps, step)
		}
	}
	candidate.Scenario.Targets = []string{"shared", "direct"}
	if _, err := Compare(baseline, candidate); err == nil || !strings.Contains(err.Error(), "target order") {
		t.Fatalf("expected endpoint-order incompatibility, got %v", err)
	}
}

func TestReadResultBoundsInputSize(t *testing.T) {
	path := filepath.Join(t.TempDir(), "large-result.json")
	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := file.Truncate((4 << 20) + 1); err != nil {
		file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadResult(path); err == nil || !strings.Contains(err.Error(), "4 MiB") {
		t.Fatalf("expected bounded input rejection, got %v", err)
	}
}
