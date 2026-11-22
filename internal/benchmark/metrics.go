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
	RequestedIterations         *int64   `json:"requested_iterations"`
	CompletedIterations         int64    `json:"completed_iterations"`
	FailedIterations            int64    `json:"failed_iterations"`
	Timeouts                    *int64   `json:"timeouts"`
	SkippedIterations           int64    `json:"skipped_iterations"`
	TPS                         float64  `json:"tps_without_initial_connection,omitempty"`
	ActiveSeconds               float64  `json:"pgbench_elapsed_seconds_without_initial_connection,omitempty"`
	ErrorRate                   float64  `json:"error_rate"`
	MeanMS                      float64  `json:"successful_iteration_mean_ms"`
	P50MS                       float64  `json:"successful_iteration_p50_ms"`
	P95MS                       float64  `json:"successful_iteration_p95_ms"`
	P99MS                       float64  `json:"successful_iteration_p99_ms"`
	InitialConnectionMS         float64  `json:"initial_connection_ms,omitempty"`
	TPSIncludingConnections     *float64 `json:"tps_including_connection_times,omitempty"`
	SecondsIncludingConnections *float64 `json:"pgbench_elapsed_seconds_including_connection_times,omitempty"`
	ConnectionMeanMS            *float64 `json:"average_connection_ms,omitempty"`
	ThroughputBasis             string   `json:"throughput_basis,omitempty"`
	RateAttainment              *float64 `json:"rate_attainment,omitempty"`
	SkippedRate                 *float64 `json:"skipped_rate,omitempty"`
	ScheduleLagMeanMS           *float64 `json:"successful_iteration_schedule_lag_mean_ms,omitempty"`
	ScheduleLagMaxMS            *float64 `json:"successful_iteration_schedule_lag_max_ms,omitempty"`
	LatencyBasis                string   `json:"latency_basis,omitempty"`
}

func readMetrics(summary, prefix string, rate float64, mode string) (*Metrics, error) {
	if regexp.MustCompile(`(?m)^number of transactions actually processed:\s*0\s*$`).MatchString(summary) {
		return nil, fmt.Errorf("pgbench completed no iterations; increase duration to allow for connection setup")
	}
	patterns := []string{
		`(?m)^number of transactions actually processed:\s*(\d+)(?:/\d+)?\s*$`,
		`(?m)^number of failed transactions:\s*(\d+)`,
		`(?m)^tps = ([0-9.]+) \(without initial connection time\)`,
		`(?m)^initial connection time = ([0-9.]+) ms`,
	}
	if mode != "persistent" {
		patterns[2] = `(?m)^tps = ([0-9.]+) \(including reconnection times\)`
		patterns[3] = `(?m)^average connection time = ([0-9.]+) ms`
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
	var skipped int64
	if rate > 0 {
		match := regexp.MustCompile(`(?m)^number of transactions skipped:\s*(\d+)`).FindStringSubmatch(summary)
		if len(match) != 2 {
			return nil, fmt.Errorf("rate summary is missing skipped transaction count")
		}
		skipped, err = strconv.ParseInt(match[1], 10, 64)
		if err != nil || skipped > math.MaxInt64-completed-failed {
			return nil, fmt.Errorf("invalid skipped iteration count")
		}
	}
	entries, err := os.ReadDir(filepath.Dir(prefix))
	if err != nil {
		return nil, fmt.Errorf("pgbench transaction logs are missing")
	}
	var latencies []int64
	var logCounts latencyLogCounts
	var totalBytes int64
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
		if err := readLatencyLog(path, &latencies, &logCounts, rate > 0); err != nil {
			return nil, err
		}
	}
	if int64(len(latencies)) != completed || logCounts.failed != failed || logCounts.skipped != skipped {
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
	metrics := &Metrics{
		CompletedIterations: completed, FailedIterations: failed,
		TPS: tps, ActiveSeconds: float64(completed) / tps, ErrorRate: float64(failed) / float64(completed+failed),
		MeanMS: sum / float64(completed) / 1000, P50MS: percentile(0.50), P95MS: percentile(0.95), P99MS: percentile(0.99),
		InitialConnectionMS: connection,
	}
	if mode != "persistent" {
		elapsed := float64(completed) / tps
		metrics.TPS, metrics.ActiveSeconds, metrics.InitialConnectionMS = 0, 0, 0
		metrics.TPSIncludingConnections = &tps
		metrics.SecondsIncludingConnections = &elapsed
		metrics.ConnectionMeanMS = &connection
		metrics.ThroughputBasis = "including-connection-times"
		metrics.LatencyBasis = "transaction-start-to-completion"
	}
	if rate > 0 {
		requested := completed + failed + skipped
		attainment := tps / rate
		skippedRate := float64(skipped) / float64(requested)
		lagMean, lagMax := logCounts.lagSum/float64(completed)/1000, float64(logCounts.lagMax)/1000
		metrics.RequestedIterations = &requested
		metrics.SkippedIterations = skipped
		metrics.RateAttainment = &attainment
		metrics.SkippedRate = &skippedRate
		metrics.ScheduleLagMeanMS = &lagMean
		metrics.ScheduleLagMaxMS = &lagMax
		metrics.LatencyBasis = "scheduled-start-to-completion"
	}
	return metrics, nil
}

type latencyLogCounts struct {
	failed, skipped int64
	lagSum          float64
	lagMax          int64
}

func readLatencyLog(path string, latencies *[]int64, counts *latencyLogCounts, rate bool) error {
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
		var lag int64
		if rate {
			if len(fields) != 7 {
				return fmt.Errorf("rate transaction log requires schedule lag")
			}
			var err error
			lag, err = strconv.ParseInt(fields[6], 10, 64)
			if err != nil || lag < 0 {
				return fmt.Errorf("invalid transaction schedule lag")
			}
		}
		if fields[2] == "skipped" && rate {
			counts.skipped++
			continue
		}
		if fields[2] == "failed" {
			counts.failed++
			continue
		}
		latency, err := strconv.ParseInt(fields[2], 10, 64)
		if err != nil || latency < 0 {
			return fmt.Errorf("invalid pgbench transaction latency")
		}
		if rate {
			if lag > latency {
				return fmt.Errorf("schedule lag exceeds scheduled-start latency")
			}
			counts.lagSum += float64(lag)
			if lag > counts.lagMax {
				counts.lagMax = lag
			}
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
	if metrics.throughput() < limits.MinTPS {
		failures = append(failures, "throughput is below min_tps")
	}
	if metrics.SkippedRate != nil && *metrics.SkippedRate > limits.MaxSkippedRate {
		failures = append(failures, "skipped rate exceeds max_skipped_rate")
	}
	if metrics.RateAttainment != nil && *metrics.RateAttainment < limits.MinRateAttainment {
		failures = append(failures, "achieved rate is below min_rate_attainment")
	}
	if metrics.ConnectionMeanMS != nil && limits.MaxConnectionMeanMS > 0 && *metrics.ConnectionMeanMS > limits.MaxConnectionMeanMS {
		failures = append(failures, "average connection time exceeds max_connection_mean_ms")
	}
	return failures
}
