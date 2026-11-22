package benchmark

import (
	"bufio"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

type Metrics struct {
	RequestedIterations *int64  `json:"requested_iterations"`
	CompletedIterations int64   `json:"completed_iterations"`
	FailedIterations    int64   `json:"failed_iterations"`
	Timeouts            *int64  `json:"timeouts"`
	SkippedIterations   int64   `json:"skipped_iterations"`
	TPS                 float64 `json:"tps_without_initial_connection"`
	ActiveSeconds       float64 `json:"pgbench_elapsed_seconds_without_initial_connection"`
	ErrorRate           float64 `json:"error_rate"`
	MeanMS              float64 `json:"successful_iteration_mean_ms"`
	P50MS               float64 `json:"successful_iteration_p50_ms"`
	P95MS               float64 `json:"successful_iteration_p95_ms"`
	P99MS               float64 `json:"successful_iteration_p99_ms"`
	InitialConnectionMS float64 `json:"initial_connection_ms"`
}

func readMetrics(summary, prefix string) (*Metrics, error) {
	if regexp.MustCompile(`(?m)^number of transactions actually processed:\s*0\s*$`).MatchString(summary) {
		return nil, fmt.Errorf("pgbench completed no iterations; increase duration to allow for connection setup")
	}
	patterns := []string{
		`(?m)^number of transactions actually processed:\s*(\d+)(?:/\d+)?\s*$`,
		`(?m)^number of failed transactions:\s*(\d+)`,
		`(?m)^tps = ([0-9.]+) \(without initial connection time\)`,
		`(?m)^initial connection time = ([0-9.]+) ms`,
	}
	values := make([]string, len(patterns))
	for index, pattern := range patterns {
		match := regexp.MustCompile(pattern).FindStringSubmatch(summary)
		if len(match) != 2 {
			return nil, fmt.Errorf("pgbench summary is missing required measurement %d", index+1)
		}
		values[index] = match[1]
	}
	completed, err := strconv.ParseInt(values[0], 10, 64)
	if err != nil || completed < 1 {
		return nil, fmt.Errorf("pgbench did not report positive completed iterations")
	}
	failed, err := strconv.ParseInt(values[1], 10, 64)
	if err != nil || failed > math.MaxInt64-completed {
		return nil, fmt.Errorf("invalid failed iteration count")
	}
	tps, err := strconv.ParseFloat(values[2], 64)
	if err != nil || !finite(tps) || tps <= 0 {
		return nil, fmt.Errorf("invalid pgbench throughput")
	}
	connection, err := strconv.ParseFloat(values[3], 64)
	if err != nil || !finite(connection) || connection < 0 {
		return nil, fmt.Errorf("invalid pgbench connection timing")
	}
	entries, err := os.ReadDir(filepath.Dir(prefix))
	if err != nil {
		return nil, fmt.Errorf("pgbench transaction logs are missing")
	}
	var latencies []int64
	var failedLogs, totalBytes int64
	for _, entry := range entries {
		if !strings.HasPrefix(entry.Name(), filepath.Base(prefix)+".") {
			continue
		}
		path := filepath.Join(filepath.Dir(prefix), entry.Name())
		info, err := os.Stat(path)
		if err != nil {
			return nil, err
		}
		totalBytes += info.Size()
		if totalBytes > 64<<20 {
			return nil, fmt.Errorf("transaction logs exceed the 64 MiB analysis limit; use shorter measurements")
		}
		if err := readLatencyLog(path, &latencies, &failedLogs); err != nil {
			return nil, err
		}
	}
	if int64(len(latencies)) != completed || failedLogs != failed {
		return nil, fmt.Errorf("transaction logs disagree with pgbench summary counts")
	}
	sort.Slice(latencies, func(first, second int) bool { return latencies[first] < latencies[second] })
	var sum float64
	for _, latency := range latencies {
		sum += float64(latency)
	}
	percentile := func(fraction float64) float64 {
		return float64(latencies[int(math.Ceil(fraction*float64(len(latencies))))-1]) / 1000
	}
	return &Metrics{
		CompletedIterations: completed, FailedIterations: failed,
		TPS: tps, ActiveSeconds: float64(completed) / tps, ErrorRate: float64(failed) / float64(completed+failed),
		MeanMS: sum / float64(completed) / 1000, P50MS: percentile(0.50), P95MS: percentile(0.95), P99MS: percentile(0.99),
		InitialConnectionMS: connection,
	}, nil
}

func readLatencyLog(path string, latencies *[]int64, failed *int64) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) < 6 {
			return fmt.Errorf("invalid pgbench transaction log row")
		}
		if fields[2] == "failed" {
			*failed = *failed + 1
			continue
		}
		latency, err := strconv.ParseInt(fields[2], 10, 64)
		if err != nil || latency < 0 {
			return fmt.Errorf("invalid pgbench transaction latency")
		}
		if len(*latencies) >= 1_000_000 {
			return fmt.Errorf("transaction logs exceed one million iterations; use shorter measurements")
		}
		*latencies = append(*latencies, latency)
	}
	return scanner.Err()
}

func thresholdFailures(metrics *Metrics, limits Thresholds) []string {
	var failures []string
	if metrics.P95MS > limits.MaxP95MS {
		failures = append(failures, "p95 latency exceeds max_p95_ms")
	}
	if metrics.ErrorRate > limits.MaxErrorRate {
		failures = append(failures, "error rate exceeds max_error_rate")
	}
	if metrics.TPS < limits.MinTPS {
		failures = append(failures, "throughput is below min_tps")
	}
	return failures
}
