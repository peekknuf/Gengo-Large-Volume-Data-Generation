package benchmark

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const measurementSummary = `number of transactions actually processed: 4
number of failed transactions: 1 (20.000%)
latency average = 3.750 ms
initial connection time = 12.500 ms
tps = 125.000000 (without initial connection time)
`

func TestReadMetrics(t *testing.T) {
	prefix := filepath.Join(t.TempDir(), "transactions")
	for suffix, contents := range map[string]string{
		".1": "0 1 1000 0 1700000000 0\n0 2 9000 0 1700000000 0\n0 3 failed 0 1700000000 0\n",
		".2": "1 1 2000 0 1700000000 0\n1 2 3000 0 1700000000 0\n",
	} {
		if err := os.WriteFile(prefix+suffix, []byte(contents), 0600); err != nil {
			t.Fatal(err)
		}
	}
	metrics, err := readMetrics(measurementSummary, prefix)
	if err != nil {
		t.Fatal(err)
	}
	if metrics.CompletedIterations != 4 || metrics.FailedIterations != 1 || metrics.ErrorRate != 0.2 || metrics.MeanMS != 3.75 || metrics.P50MS != 2 || metrics.P95MS != 9 || metrics.P99MS != 9 || metrics.TPS != 125 || metrics.InitialConnectionMS != 12.5 || metrics.RequestedIterations != nil || metrics.Timeouts != nil {
		t.Fatalf("incorrect measurements: %+v", metrics)
	}
	if failures := thresholdFailures(metrics, Thresholds{MaxP95MS: 8, MaxErrorRate: 0.1, MinTPS: 126}); len(failures) != 3 {
		t.Fatalf("threshold failures = %v, want all three", failures)
	}
	if failures := thresholdFailures(metrics, Thresholds{MaxP95MS: 9, MaxErrorRate: 0.2, MinTPS: 125}); len(failures) != 0 {
		t.Fatalf("threshold equality should pass: %v", failures)
	}
}

func TestReadMetricsRejectsIncompleteMeasurements(t *testing.T) {
	for _, testCase := range []struct{ name, summary, log string }{
		{"missing_logs", measurementSummary, ""},
		{"count_mismatch", measurementSummary, "0 1 1000 0 1700000000 0\n"},
		{"bad_row", measurementSummary, "0 1 1000\n"},
		{"negative_latency", measurementSummary, "0 1 -1000 0 1700000000 0\n"},
		{"missing_summary", "", "0 1 1000 0 1700000000 0\n"},
		{"zero_completed", strings.Replace(measurementSummary, "processed: 4", "processed: 0", 1), "0 1 1000 0 1700000000 0\n"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			prefix := filepath.Join(t.TempDir(), "transactions")
			if testCase.log != "" {
				if err := os.WriteFile(prefix+".1", []byte(testCase.log), 0600); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := readMetrics(testCase.summary, prefix); err == nil {
				t.Fatal("accepted missing or inconsistent measurements")
			}
		})
	}
}

func TestReadMetricsBoundsLogAnalysis(t *testing.T) {
	prefix := filepath.Join(t.TempDir(), "transactions")
	file, err := os.Create(prefix + ".1")
	if err != nil {
		t.Fatal(err)
	}
	if err := file.Truncate((64 << 20) + 1); err != nil {
		file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := readMetrics(measurementSummary, prefix); err == nil || !strings.Contains(err.Error(), "64 MiB") {
		t.Fatalf("expected bounded log analysis, got %v", err)
	}
}
