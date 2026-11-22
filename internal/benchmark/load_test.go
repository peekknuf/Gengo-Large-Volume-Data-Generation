package benchmark

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const rateSummary = `number of transactions actually processed: 4
number of failed transactions: 1 (20.000%)
number of transactions skipped: 5 (50.000%)
initial connection time = 12.500 ms
tps = 2.000000 (without initial connection time)
`

const rateLog = "0 1 1000 0 1700000000 0 100\n0 2 2000 0 1700000000 0 200\n0 3 3000 0 1700000000 0 300\n0 4 9000 0 1700000000 0 400\n0 5 failed 0 1700000000 0 0\n0 5 skipped 0 1700000000 0 500\n0 5 skipped 0 1700000000 0 500\n0 5 skipped 0 1700000000 0 500\n0 5 skipped 0 1700000000 0 500\n0 5 skipped 0 1700000000 0 500\n"

func arrivalScenario(t *testing.T) string {
	t.Helper()
	path := customScenario(t, "SELECT 1;", false)
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	scenario := strings.ReplaceAll(string(contents), "concurrency: [1, 2]", "clients: 1\narrival_rates: [2, 100]\nlatency_limit: 1s")
	scenario = strings.ReplaceAll(scenario, "max_error_rate: 0.01", "max_error_rate: 0.2")
	scenario = strings.ReplaceAll(scenario, "min_tps: 10", "min_tps: 0\n  max_skipped_rate: 0.5\n  min_rate_attainment: 0.5")
	if err := os.WriteFile(path, []byte(scenario), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestArrivalScenarioValidation(t *testing.T) {
	for _, test := range []struct{ old, replacement string }{
		{"clients: 1", "clients: 0"},
		{"clients: 1", "clients: 1025"},
		{"clients: 1", "clients: 1\nconcurrency: [1]"},
		{"arrival_rates: [2, 100]", "arrival_rates: []"},
		{"arrival_rates: [2, 100]", "arrival_rates: [0, 1]"},
		{"arrival_rates: [2, 100]", "arrival_rates: [0.01, 1]"},
		{"arrival_rates: [2, 100]", "arrival_rates: [2, 2]"},
		{"arrival_rates: [2, 100]", "arrival_rates: [100, 2]"},
		{"arrival_rates: [2, 100]", "arrival_rates: [.nan]"},
		{"arrival_rates: [2, 100]", "arrival_rates: [.inf]"},
		{"arrival_rates: [2, 100]", "arrival_rates: [1000001]"},
		{"latency_limit: 1s", "latency_limit: 0s"},
		{"latency_limit: 1s", "latency_limit: 31s"},
		{"latency_limit: 1s", "latency_limit: 0.1ms"},
		{"min_rate_attainment: 0.5", "min_rate_attainment: 0"},
		{"min_rate_attainment: 0.5", "min_rate_attainment: 1.1"},
		{"max_skipped_rate: 0.5", "max_skipped_rate: -1"},
	} {
		path := arrivalScenario(t)
		contents, _ := os.ReadFile(path)
		if err := os.WriteFile(path, []byte(strings.ReplaceAll(string(contents), test.old, test.replacement)), 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := LoadScenario(path); err == nil {
			t.Fatalf("accepted %s", test.replacement)
		}
	}
	if _, err := LoadScenario(scenarioFixture(t, validScenario+"clients: 1\n")); err == nil {
		t.Fatal("accepted rate client count for concurrency sweep")
	}
	path := arrivalScenario(t)
	plan, err := LoadScenario(path)
	if err != nil || plan.Scenario.Clients != 1 || len(loadLevels(plan.Scenario)) != 2 || loadLevels(plan.Scenario)[1].rate != 100 {
		t.Fatalf("arrival scenario did not load: %+v, %v", plan, err)
	}
}

func TestRateMetrics(t *testing.T) {
	prefix := filepath.Join(t.TempDir(), "transactions")
	if err := os.WriteFile(prefix+".1", []byte(rateLog), 0600); err != nil {
		t.Fatal(err)
	}
	metrics, err := readMetrics(rateSummary, prefix, 10, "persistent")
	if err != nil {
		t.Fatal(err)
	}
	if *metrics.RequestedIterations != 10 || metrics.CompletedIterations != 4 || metrics.FailedIterations != 1 || metrics.SkippedIterations != 5 || *metrics.SkippedRate != 0.5 || *metrics.RateAttainment != 0.2 || *metrics.ScheduleLagMeanMS != 0.25 || *metrics.ScheduleLagMaxMS != 0.4 || metrics.P95MS != 9 || metrics.ErrorRate != 0.2 || metrics.LatencyBasis != "scheduled-start-to-completion" {
		t.Fatalf("incorrect rate measurements: %+v", metrics)
	}
	limits := Thresholds{MaxP95MS: 9, MaxErrorRate: 0.2, MinTPS: 2, MaxSkippedRate: 0.5, MinRateAttainment: 0.2}
	if failures := thresholdFailures(metrics, limits); len(failures) != 0 {
		t.Fatalf("equality should pass: %v", failures)
	}
	limits.MaxSkippedRate, limits.MinRateAttainment = 0.4, 0.3
	if failures := thresholdFailures(metrics, limits); len(failures) != 2 {
		t.Fatalf("missing skipped-work and attainment failures: %v", failures)
	}
}

func TestRateMetricsRejectIncompleteLogs(t *testing.T) {
	for _, test := range []struct{ summary, log string }{
		{rateSummary, strings.ReplaceAll(rateLog, "skipped", "1000")},
		{rateSummary, strings.ReplaceAll(rateLog, " 100\n", "\n")},
		{rateSummary, strings.ReplaceAll(rateLog, " 100\n", " -1\n")},
		{rateSummary, strings.ReplaceAll(rateLog, " 100\n", " 1001\n")},
		{strings.ReplaceAll(rateSummary, "number of transactions skipped: 5 (50.000%)\n", ""), rateLog},
		{strings.ReplaceAll(rateSummary, "skipped: 5", "skipped: 9223372036854775807"), rateLog},
	} {
		prefix := filepath.Join(t.TempDir(), "transactions")
		if err := os.WriteFile(prefix+".1", []byte(test.log), 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := readMetrics(test.summary, prefix, 10, "persistent"); err == nil {
			t.Fatal("accepted incomplete rate measurement")
		}
	}
}

func TestRateSweepAndComparison(t *testing.T) {
	installClientHelpers(t)
	t.Setenv("GENGO_BENCH_HELPER", "arrival")
	plan, err := LoadScenario(arrivalScenario(t))
	if err != nil {
		t.Fatal(err)
	}
	output := filepath.Join(t.TempDir(), "result")
	result, err := Run(t.Context(), plan, "", output, &bytes.Buffer{})
	if err == nil || result == nil || len(result.Steps) != 6 {
		t.Fatalf("expected continued sweep after rate threshold failures: %+v, %v", result, err)
	}
	for _, step := range result.Steps {
		if (step.ArrivalRate == 2 && step.Status != "passed") || (step.ArrivalRate == 100 && step.Status != "failed") || result.RateCapacity[step.Target] != 2 {
			t.Fatalf("incorrect rate thresholds/capacity: %+v", step)
		}
	}
	loaded, err := ReadResult(filepath.Join(output, "result.json"))
	if err != nil {
		t.Fatal(err)
	}
	comparison, err := Compare(result, loaded)
	if err != nil || comparison.Steps[1].ArrivalRate != 100 || comparison.RateCapacity[0].Baseline != 2 || comparison.Steps[0].SkippedRate.Delta != 0 {
		t.Fatalf("rate comparison failed: %+v, %v", comparison, err)
	}
	var summary, report bytes.Buffer
	PrintSummary(&summary, result)
	PrintComparison(&report, comparison)
	if !strings.Contains(summary.String(), "highest passing tested arrival rate: 2 iterations/s") || !strings.Contains(report.String(), "highest passing tested arrival rate: 2 baseline, 2 candidate") || !strings.Contains(report.String(), "Skipped rate: 50.00% to 50.00%") {
		t.Fatalf("rate report omitted load or skipped-work measurements:\n%s\n%s", summary.String(), report.String())
	}
	saved, err := os.ReadFile(filepath.Join(output, "result.json"))
	if err != nil {
		t.Fatal(err)
	}
	missing := filepath.Join(t.TempDir(), "missing.json")
	if err := os.WriteFile(missing, bytes.ReplaceAll(saved, []byte(`"skipped_iterations": 5,`), nil), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadResult(missing); err == nil || !strings.Contains(err.Error(), "missing required field skipped_iterations") {
		t.Fatalf("missing skipped measurement was not identified: %v", err)
	}
	loaded.Scenario.LatencyLimit = "1000ms"
	if _, err := Compare(result, loaded); err != nil {
		t.Fatalf("equivalent latency limit rejected: %v", err)
	}
	loaded.Scenario.LatencyLimit = "2s"
	if _, err := Compare(result, loaded); err == nil || !strings.Contains(err.Error(), "latency limit") {
		t.Fatalf("different latency limit compared: %v", err)
	}
	loaded.Scenario.LatencyLimit = "1s"
	loaded.Steps[0], loaded.Steps[1] = loaded.Steps[1], loaded.Steps[0]
	if compared, err := Compare(result, loaded); err != nil || compared.Steps[0].ArrivalRate != 2 || compared.Steps[1].ArrivalRate != 100 {
		t.Fatalf("rate steps were not paired by target/client/rate: %+v, %v", compared, err)
	}
	loaded.Steps[0], loaded.Steps[1] = loaded.Steps[1], loaded.Steps[0]
	loaded.Scenario.Clients = 2
	for index := range loaded.Steps {
		loaded.Steps[index].Concurrency = 2
	}
	if _, err := Compare(result, loaded); err == nil || !strings.Contains(err.Error(), "rate client count") {
		t.Fatalf("different client budget compared: %v", err)
	}
	loaded.Scenario.Clients = 1
	for index := range loaded.Steps {
		loaded.Steps[index].Concurrency = 1
	}
	loaded.RateCapacity["direct"] = 100
	if _, err := Compare(result, loaded); err == nil {
		t.Fatal("inconsistent rate capacity compared")
	}
	loaded.RateCapacity["direct"] = 2
	*loaded.Steps[0].Measurement.Metrics.RequestedIterations = 99
	if _, err := Compare(result, loaded); err == nil {
		t.Fatal("inconsistent scheduled counts compared")
	}
	*loaded.Steps[0].Measurement.Metrics.RequestedIterations = 10
	loaded.Steps[0].Measurement.Metrics.ScheduleLagMeanMS = nil
	if _, err := Compare(result, loaded); err == nil {
		t.Fatal("missing schedule lag compared")
	}
}
