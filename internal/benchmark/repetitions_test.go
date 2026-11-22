package benchmark

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestRoundSchedule(t *testing.T) {
	count := 4
	scenario := Scenario{Targets: []string{"direct", "shared", "dedicated"}, Repetitions: &count, TargetOrder: "rotate"}
	want := []Round{
		{1, []string{"direct", "shared", "dedicated"}},
		{2, []string{"shared", "dedicated", "direct"}},
		{3, []string{"dedicated", "direct", "shared"}},
		{4, []string{"direct", "shared", "dedicated"}},
	}
	if got := roundSchedule(scenario); !reflect.DeepEqual(got, want) {
		t.Fatalf("rotation:\ngot %+v\nwant %+v", got, want)
	}
	scenario.TargetOrder = "fixed"
	for _, round := range roundSchedule(scenario) {
		if !reflect.DeepEqual(round.Targets, []string{"direct", "shared", "dedicated"}) {
			t.Fatalf("fixed order mutated: %+v", round)
		}
	}
}

func repeatedFixture(t *testing.T) *Result {
	t.Helper()
	result := comparisonFixture(t)
	count := 3
	result.Scenario.Repetitions = &count
	result.Scenario.Concurrency = []int{1}
	result.Schedule = roundSchedule(result.Scenario)
	result.Steps = nil
	for index, tps := range []float64{10, 20, 90} {
		metrics := &Metrics{CompletedIterations: 100, TPS: tps, P95MS: []float64{1, 5, 300}[index]}
		failures := thresholdFailures(metrics, result.Scenario.Thresholds)
		status := "passed"
		if len(failures) > 0 {
			status = "failed"
		}
		result.Steps = append(result.Steps, Step{Repetition: index + 1, Target: "direct", Concurrency: 1, Status: status, Failures: failures, Measurement: &Phase{Status: "completed", Metrics: metrics}})
	}
	result.Status = "failed"
	refreshAggregates(result)
	return result
}

func TestRepetitionStatisticsAndCapacity(t *testing.T) {
	result := repeatedFixture(t)
	aggregate := result.Aggregates[0]
	if aggregate.Status != "failed" || aggregate.Passed != 2 || aggregate.Failed != 1 || aggregate.Measured != 3 || aggregate.PassRate != 2.0/3 || *aggregate.TPS != (Distribution{10, 20, 90}) || *aggregate.P95MS != (Distribution{1, 5, 300}) || result.Capacity["direct"] != 0 {
		t.Fatalf("incorrect repeated statistics or strict capacity: %+v", result)
	}
	minimum := 2.0 / 3
	result.Scenario.MinPassRate = &minimum
	refreshAggregates(result)
	if result.Aggregates[0].Status != "passed" || result.Capacity["direct"] != 1 {
		t.Fatalf("explicit pass-rate rule ignored: %+v", result)
	}
	result.Steps[2].Status = "error"
	refreshAggregates(result)
	if result.Aggregates[0].Status != "error" || result.Aggregates[0].Errors != 1 || result.Capacity["direct"] != 0 {
		t.Fatal("execution error qualified for capacity")
	}
	result.Steps = result.Steps[:2]
	refreshAggregates(result)
	if result.Aggregates[0].Status != "incomplete" || result.Capacity["direct"] != 0 {
		t.Fatal("missing repetition qualified for capacity")
	}
	if got := distribution([]float64{40, 10, 20, 30}); *got != (Distribution{10, 25, 40}) {
		t.Fatalf("incorrect even-sample median: %+v", got)
	}
}

func TestRepetitionScenarioValidation(t *testing.T) {
	for _, fields := range []string{
		"repetitions: 0\n", "repetitions: 31\n", "repetitions: -1\n",
		"target_order: random\n", "min_pass_rate: 0\n", "min_pass_rate: 1.1\n", "min_pass_rate: .nan\n",
	} {
		if _, err := LoadScenario(scenarioFixture(t, validScenario+fields)); err == nil {
			t.Fatalf("accepted invalid repetition controls: %s", fields)
		}
	}
	path := scenarioFixture(t, validScenario+"repetitions: 3\ntarget_order: rotate\nmin_pass_rate: 1\n")
	plan, err := LoadScenario(path)
	if err != nil || repetitions(plan.Scenario) != 3 || targetOrder(plan.Scenario) != "rotate" || minPassRate(plan.Scenario) != 1 {
		t.Fatalf("repetition controls not loaded: %+v, %v", plan, err)
	}
	count := 30
	scenario := plan.Scenario
	scenario.Repetitions = &count
	for clients := 3; clients <= 32; clients++ {
		scenario.Concurrency = append(scenario.Concurrency, clients)
	}
	if err := validateRepetitions(scenario); err == nil {
		t.Fatal("accepted an experiment exceeding the sample budget")
	}
}

func TestRepeatedSweepExecution(t *testing.T) {
	installClientHelpers(t)
	t.Setenv("GENGO_BENCH_HELPER", "success")
	path := scenarioFixture(t, strings.ReplaceAll(validScenario, "max_error_rate: 0.01", "max_error_rate: 0.2")+"repetitions: 3\ntarget_order: rotate\n")
	plan, err := LoadScenario(path)
	if err != nil {
		t.Fatal(err)
	}
	output := filepath.Join(t.TempDir(), "result")
	result, err := Run(t.Context(), plan, "", output, &bytes.Buffer{})
	if err != nil || result == nil || len(result.Steps) != 18 || len(result.Aggregates) != 6 {
		t.Fatalf("incomplete repeated sweep: %+v, %v", result, err)
	}
	index := 0
	for _, round := range result.Schedule {
		for _, target := range round.Targets {
			for _, clients := range []int{1, 2} {
				step := result.Steps[index]
				if step.Repetition != round.Repetition || step.Target != target || step.Concurrency != clients || step.Warmup == nil || step.Warmup.Status != "completed" {
					t.Fatalf("wrong actual execution order: %+v", step)
				}
				index++
			}
		}
	}
	if _, err := os.Stat(filepath.Join(output, "rep-3-dedicated-c2-measurement", "pgbench.txt")); err != nil {
		t.Fatal(err)
	}
	for _, aggregate := range result.Aggregates {
		if aggregate.Passed != 3 || aggregate.Status != "passed" || result.Capacity[aggregate.Target] != 2 {
			t.Fatalf("incorrect repeated capacity: %+v", aggregate)
		}
	}
	if _, err := ReadResult(filepath.Join(output, "result.json")); err != nil {
		t.Fatal(err)
	}
}

func TestRepeatedPassRuleControlsExit(t *testing.T) {
	installClientHelpers(t)
	t.Setenv("GENGO_BENCH_HELPER", "repeat-threshold")
	for _, relaxed := range []bool{false, true} {
		path := scenarioFixture(t, strings.ReplaceAll(validScenario, "max_error_rate: 0.01", "max_error_rate: 0.2")+"repetitions: 3\n")
		plan, err := LoadScenario(path)
		if err != nil {
			t.Fatal(err)
		}
		plan.Scenario.Targets = []string{"direct"}
		if relaxed {
			minimum := 2.0 / 3
			plan.Scenario.MinPassRate = &minimum
		}
		output := filepath.Join(t.TempDir(), "result")
		result, err := Run(t.Context(), plan, "", output, &bytes.Buffer{})
		if (err == nil) != relaxed || result == nil || len(result.Steps) != 6 {
			t.Fatalf("wrong repeated experiment exit: relaxed=%t result=%+v error=%v", relaxed, result, err)
		}
		for _, aggregate := range result.Aggregates {
			if aggregate.Passed != 2 || aggregate.Failed != 1 || (aggregate.Status == "passed") != relaxed {
				t.Fatalf("wrong pass rule: %+v", aggregate)
			}
		}
		if relaxed && result.Capacity["direct"] != 2 {
			t.Fatal("relaxed pass rule lost capacity")
		}
		if !relaxed && result.Capacity["direct"] != 0 {
			t.Fatal("strict pass rule accepted a flaky load")
		}
		if _, err := ReadResult(filepath.Join(output, "result.json")); err != nil {
			t.Fatal(err)
		}
	}
}

func TestRepeatedLifecycleIsolation(t *testing.T) {
	installClientHelpers(t)
	t.Setenv("GENGO_BENCH_HELPER", "state")
	events := filepath.Join(t.TempDir(), "events.txt")
	t.Setenv("GENGO_STATE_EVENTS", events)
	path := customScenario(t, "SELECT 1;", false)
	for _, name := range []string{"setup.sql", "reset.sql"} {
		if err := os.WriteFile(filepath.Join(filepath.Dir(path), name), []byte("SELECT 1;"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	contents, _ := os.ReadFile(path)
	contents = append(contents, []byte("repetitions: 3\naccess_mode: read-write\nstate_policy: reset-per-phase\nsetup_file: setup.sql\nreset_file: reset.sql\n")...)
	if err := os.WriteFile(path, contents, 0600); err != nil {
		t.Fatal(err)
	}
	plan, err := LoadScenario(path)
	if err != nil {
		t.Fatal(err)
	}
	plan.Scenario.Targets = []string{"direct"}
	plan.Scenario.Thresholds.MaxErrorRate = 0.2
	result, err := Run(t.Context(), plan, "", filepath.Join(t.TempDir(), "result"), &bytes.Buffer{})
	if err != nil || len(result.Steps) != 6 {
		t.Fatalf("repeated lifecycle: %+v, %v", result, err)
	}
	actual, err := os.ReadFile(events)
	want := "setup\n" + strings.Repeat("reset\nwarmup\nreset\nmeasurement\nreset\n", 6)
	if err != nil || string(actual) != want {
		t.Fatalf("wrong per-repetition lifecycle:\ngot %q\nwant %q\n%v", actual, want, err)
	}
}

func TestRepeatedComparisonAndValidation(t *testing.T) {
	baseline, candidate := repeatedFixture(t), repeatedFixture(t)
	candidate.Steps[0].Measurement.Metrics.TPS = 15
	candidate.Steps[1].Measurement.Metrics.TPS = 30
	candidate.Steps[2].Measurement.Metrics.TPS = 150
	refreshAggregates(candidate)
	comparison, err := Compare(baseline, candidate)
	if err != nil || len(comparison.Steps) != 1 || comparison.Steps[0].TPS.Delta != 10 || comparison.Steps[0].BaselinePassed != 2 || len(comparison.Baseline.Aggregates) != 1 || comparison.Candidate.Aggregates[0].TPS.Max != 150 {
		t.Fatalf("wrong aggregate comparison: %+v, %v", comparison, err)
	}
	var report bytes.Buffer
	PrintSummary(&report, baseline)
	PrintComparison(&report, comparison)
	if !strings.Contains(report.String(), "per-repetition p95 median 5.00") || !strings.Contains(report.String(), "Passing repetitions: 2/3 baseline, 2/3 candidate") {
		t.Fatalf("missing variation report: %s", report.String())
	}
	for _, mutate := range []func(*Result){
		func(result *Result) { result.Steps[1].Repetition = 1 },
		func(result *Result) { result.Steps[0].Repetition = 0 },
		func(result *Result) { result.Schedule = nil },
		func(result *Result) { result.Schedule[0].Targets = []string{"shared"} },
		func(result *Result) { result.Aggregates = nil },
		func(result *Result) { result.Aggregates[0].TPS.Median = 123 },
		func(result *Result) { result.Aggregates[0].Passed = 3 },
		func(result *Result) { result.Steps = result.Steps[:2] },
	} {
		broken := repeatedFixture(t)
		mutate(broken)
		if _, err := ReadResult(saveComparisonFixture(t, broken)); err == nil {
			t.Fatal("accepted inconsistent repeated experiment")
		}
	}
	minimum := 2.0 / 3
	candidate.Scenario.MinPassRate = &minimum
	candidate.Status = "completed"
	refreshAggregates(candidate)
	if _, err := Compare(baseline, candidate); err == nil || !strings.Contains(err.Error(), "repetition pass rule") {
		t.Fatalf("different pass rules compared: %v", err)
	}
}

func TestRepeatedArrivalCapacity(t *testing.T) {
	installClientHelpers(t)
	t.Setenv("GENGO_BENCH_HELPER", "arrival")
	plan, err := LoadScenario(arrivalScenario(t))
	if err != nil {
		t.Fatal(err)
	}
	count := 3
	plan.Scenario.Repetitions, plan.Scenario.TargetOrder = &count, "rotate"
	output := filepath.Join(t.TempDir(), "result")
	result, err := Run(t.Context(), plan, "", output, &bytes.Buffer{})
	if err == nil || result == nil || len(result.Steps) != 18 || len(result.Aggregates) != 6 || len(result.Capacity) != 0 {
		t.Fatalf("incomplete repeated arrival sweep: %+v, %v", result, err)
	}
	for _, aggregate := range result.Aggregates {
		if result.RateCapacity[aggregate.Target] != 2 || aggregate.Measured != 3 || aggregate.TPS.Median != 2 || aggregate.SkippedRate.Median != 0.5 || (aggregate.ArrivalRate == 100 && aggregate.Failed != 3) {
			t.Fatalf("incorrect repeated arrival aggregate: %+v", aggregate)
		}
	}
	loaded, err := ReadResult(filepath.Join(output, "result.json"))
	if err != nil {
		t.Fatal(err)
	}
	comparison, err := Compare(result, loaded)
	if err != nil || len(comparison.Steps) != 6 || len(comparison.RateCapacity) != 3 {
		t.Fatalf("incorrect repeated arrival comparison: %+v, %v", comparison, err)
	}
}

func TestCanceledLaterRepetitionDoesNotQualify(t *testing.T) {
	installClientHelpers(t)
	t.Setenv("GENGO_BENCH_HELPER", "repeat-cancel")
	plan, err := LoadScenario(scenarioFixture(t, strings.ReplaceAll(validScenario, "max_error_rate: 0.01", "max_error_rate: 0.2")+"repetitions: 3\nmin_pass_rate: 0.25\n"))
	if err != nil {
		t.Fatal(err)
	}
	plan.Scenario.Targets = []string{"direct"}
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	result, err := Run(ctx, plan, "", filepath.Join(t.TempDir(), "result"), &bytes.Buffer{})
	if err == nil || result == nil || result.Status != "canceled" || result.Capacity["direct"] != 0 || len(result.Steps) != 3 || result.Steps[2].Repetition != 2 || result.Steps[2].Status != "canceled" {
		t.Fatalf("partial repetition qualified or lost cancellation: %+v, %v", result, err)
	}
	if result.Aggregates[0].Passed != 1 || result.Aggregates[0].Errors != 1 || result.Aggregates[1].Status != "incomplete" {
		t.Fatalf("partial results lost prior measurements: %+v", result.Aggregates)
	}
}
