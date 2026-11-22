package benchmark

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const reconnectSummary = `number of transactions actually processed: 4
number of failed transactions: 1 (20.000%)
average connection time = 0.500 ms
tps = 125.000000 (including reconnection times)
`

func TestConnectionScenarioValidation(t *testing.T) {
	for _, test := range []struct {
		fields, protocol string
		valid            bool
	}{
		{"", "simple", true},
		{"connection_mode: persistent\n", "extended", true},
		{"connection_mode: reconnect\n", "prepared", true},
		{"connection_mode: burst\n", "simple", false},
		{"connection_mode: other\n", "simple", false},
		{"", "other", false},
	} {
		contents := strings.ReplaceAll(validScenario, "protocol: simple", "protocol: "+test.protocol) + test.fields
		if _, err := LoadScenario(scenarioFixture(t, contents)); (err == nil) != test.valid {
			t.Fatalf("fields %q, protocol %q, valid=%t: %v", test.fields, test.protocol, test.valid, err)
		}
	}
	for _, invalid := range []bool{false, true} {
		contents := strings.ReplaceAll(validScenario, "warmup: 1s", "warmup: 0ms") + "connection_mode: burst\n"
		if invalid {
			contents = strings.ReplaceAll(contents, "concurrency: [1, 2]", "concurrency: [1, 129]")
		}
		if _, err := LoadScenario(scenarioFixture(t, contents)); (err != nil) != invalid {
			t.Fatalf("burst size validation: %v", err)
		}
	}
	path := arrivalScenario(t)
	contents, _ := os.ReadFile(path)
	if err := os.WriteFile(path, append(contents, []byte("connection_mode: burst\n")...), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadScenario(path); err == nil {
		t.Fatal("burst accepted arrival scheduling")
	}
}

func TestConnectionMetrics(t *testing.T) {
	prefix := filepath.Join(t.TempDir(), "transactions")
	logs := "0 1 1000 0 1700000000 0\n0 2 2000 0 1700000000 0\n0 3 3000 0 1700000000 0\n0 4 9000 0 1700000000 0\n0 5 failed 0 1700000000 0\n"
	if err := os.WriteFile(prefix+".1", []byte(logs), 0600); err != nil {
		t.Fatal(err)
	}
	metrics, err := readMetrics(reconnectSummary, prefix, 0, "reconnect")
	if err != nil {
		t.Fatal(err)
	}
	if metrics.throughput() != 125 || metrics.TPS != 0 || metrics.ActiveSeconds != 0 || *metrics.ConnectionMeanMS != 0.5 || *metrics.SecondsIncludingConnections != 0.032 || metrics.P95MS != 9 || metrics.LatencyBasis != "transaction-start-to-completion" {
		t.Fatalf("incorrect reconnect measurements: %+v", metrics)
	}
	if failures := thresholdFailures(metrics, Thresholds{MaxP95MS: 9, MaxErrorRate: 0.2, MinTPS: 125}); len(failures) != 0 {
		t.Fatalf("connection throughput was ignored: %v", failures)
	}
	if failures := thresholdFailures(metrics, Thresholds{MaxP95MS: 9, MaxErrorRate: 0.2, MinTPS: 125, MaxConnectionMeanMS: 0.5}); len(failures) != 0 {
		t.Fatalf("connection-time threshold equality failed: %v", failures)
	}
	if failures := thresholdFailures(metrics, Thresholds{MaxP95MS: 9, MaxErrorRate: 0.2, MinTPS: 125, MaxConnectionMeanMS: 0.4}); len(failures) != 1 || failures[0] != "average connection time exceeds max_connection_mean_ms" {
		t.Fatalf("connection-time threshold ignored: %v", failures)
	}
	if _, err := readMetrics(measurementSummary, prefix, 0, "reconnect"); err == nil {
		t.Fatal("reconnect accepted throughput excluding connections")
	}
	if _, err := readMetrics(reconnectSummary, prefix, 0, "persistent"); err == nil {
		t.Fatal("persistent accepted reconnect timings")
	}
}

func TestConnectionComparisons(t *testing.T) {
	baseline := comparisonFixture(t)
	candidate := comparisonFixture(t)
	candidate.Scenario.Protocol = "prepared"
	comparison, err := Compare(baseline, candidate)
	if err != nil || comparison.Candidate.Protocol != "prepared" || len(comparison.Warnings) != 3 {
		t.Fatalf("intentional protocol comparison failed: %+v, %v", comparison, err)
	}
	candidate.Scenario.ConnectionMode = "reconnect"
	for _, step := range candidate.Steps {
		metrics := step.Measurement.Metrics
		tps, elapsed, connection := metrics.TPS, float64(metrics.CompletedIterations)/metrics.TPS, float64(2)
		metrics.TPS = 0
		metrics.TPSIncludingConnections, metrics.SecondsIncludingConnections, metrics.ConnectionMeanMS = &tps, &elapsed, &connection
		metrics.ThroughputBasis, metrics.LatencyBasis = "including-connection-times", "transaction-start-to-completion"
	}
	loaded, err := ReadResult(saveComparisonFixture(t, candidate))
	if err != nil {
		t.Fatal(err)
	}
	comparison, err = Compare(baseline, loaded)
	if err != nil || len(comparison.Warnings) != 4 || comparison.Candidate.ConnectionMode != "reconnect" || comparison.Steps[0].TPS.Delta != 0 {
		t.Fatalf("intentional reconnect comparison failed: %+v, %v", comparison, err)
	}
	loaded.Steps[0].Measurement.Metrics.TPS = 10
	if _, err := Compare(baseline, loaded); err == nil {
		t.Fatal("conflicting throughput measurements compared")
	}
	loaded.Steps[0].Measurement.Metrics.TPS = 0
	loaded.Scenario.ConnectionMode = "burst"
	loaded.LoadModel = "connection-burst"
	for _, step := range loaded.Steps {
		metrics := step.Measurement.Metrics
		count := int64(step.Concurrency)
		metrics.CompletedIterations, metrics.RequestedIterations = count, &count
	}
	if _, err := Compare(baseline, loaded); err == nil || !strings.Contains(err.Error(), "load model") {
		t.Fatalf("burst compared with duration load: %v", err)
	}
	*loaded.Steps[0].Measurement.Metrics.RequestedIterations = 99
	if _, err := Compare(loaded, loaded); err == nil || !strings.Contains(err.Error(), "burst counts") {
		t.Fatalf("partial burst compared: %v", err)
	}
}

func TestConnectionExecutionArguments(t *testing.T) {
	installClientHelpers(t)
	t.Setenv("GENGO_BENCH_HELPER", "connection")
	for _, mode := range []string{"persistent", "reconnect", "burst"} {
		for _, protocol := range []string{"simple", "extended", "prepared"} {
			path := customScenario(t, "SELECT 1;", false)
			contents, _ := os.ReadFile(path)
			text := strings.ReplaceAll(string(contents), "protocol: simple", "protocol: "+protocol)
			text = strings.ReplaceAll(text, "warmup: 1s", "warmup: 0s")
			text = strings.ReplaceAll(text, "max_error_rate: 0.01", "max_error_rate: 0.2")
			if err := os.WriteFile(path, []byte(text+"connection_mode: "+mode+"\n"), 0600); err != nil {
				t.Fatal(err)
			}
			plan, err := LoadScenario(path)
			if err != nil {
				t.Fatal(err)
			}
			t.Setenv("GENGO_CONNECTION_MODE", mode)
			t.Setenv("GENGO_CONNECTION_PROTOCOL", protocol)
			output := filepath.Join(t.TempDir(), "result")
			result, err := Run(t.Context(), plan, "", output, &bytes.Buffer{})
			if err != nil {
				t.Fatalf("%s/%s: %+v, %v", mode, protocol, result, err)
			}
			if _, err := ReadResult(filepath.Join(output, "result.json")); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func TestBurstDeadlineSavesError(t *testing.T) {
	installClientHelpers(t)
	t.Setenv("GENGO_BENCH_HELPER", "connection")
	t.Setenv("GENGO_CONNECTION_MODE", "burst")
	t.Setenv("GENGO_CONNECTION_PROTOCOL", "simple")
	t.Setenv("GENGO_CONNECTION_WAIT", "1")
	plan, err := LoadScenario(customScenario(t, "SELECT 1;", false))
	if err != nil {
		t.Fatal(err)
	}
	plan.Scenario.ConnectionMode = "burst"
	plan.Scenario.Warmup, plan.Warmup = "0s", 0
	plan.Scenario.Duration, plan.Duration = "1s", time.Second
	plan.Scenario.Concurrency, plan.Scenario.Targets = []int{1}, []string{"direct"}
	result, err := Run(t.Context(), plan, "", filepath.Join(t.TempDir(), "result"), &bytes.Buffer{})
	if err == nil || result == nil || result.Status != "failed" || len(result.Steps) != 1 || result.Steps[0].Status != "error" || result.Steps[0].Measurement.Metrics != nil || !strings.Contains(result.Steps[0].Measurement.Error, "deadline exceeded") {
		t.Fatalf("burst deadline did not persist an execution error: %+v, %v", result, err)
	}
}
