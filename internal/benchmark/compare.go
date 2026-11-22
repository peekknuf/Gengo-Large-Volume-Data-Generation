package benchmark

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"reflect"
	"slices"
	"strings"
	"time"

	"github.com/peekknuf/Gengo/internal/core"
)

type Change struct {
	Baseline  float64  `json:"baseline"`
	Candidate float64  `json:"candidate"`
	Delta     float64  `json:"delta"`
	Percent   *float64 `json:"percent_change"`
}

type ComparedStep struct {
	Target                    string  `json:"target"`
	Concurrency               int     `json:"concurrency"`
	ArrivalRate               float64 `json:"arrival_rate,omitempty"`
	BaselineStatus            string  `json:"baseline_status"`
	CandidateStatus           string  `json:"candidate_status"`
	TPS                       Change  `json:"tps"`
	P95MS                     Change  `json:"successful_iteration_p95_ms"`
	ErrorRate                 Change  `json:"error_rate"`
	ErrorRatePercentagePoints float64 `json:"error_rate_delta_percentage_points"`
	SkippedRate               *Change `json:"skipped_rate,omitempty"`
	RateAttainment            *Change `json:"rate_attainment,omitempty"`
}

type ComparedCapacity struct {
	Target    string `json:"target"`
	Baseline  int    `json:"baseline_clients"`
	Candidate int    `json:"candidate_clients"`
	Delta     int    `json:"delta_clients"`
}

type ComparedRateCapacity struct {
	Target    string  `json:"target"`
	Baseline  float64 `json:"baseline_arrival_rate"`
	Candidate float64 `json:"candidate_arrival_rate"`
	Delta     float64 `json:"delta_arrival_rate"`
}

type RunReference struct {
	Name           string                `json:"name"`
	StartedAt      time.Time             `json:"started_at"`
	Status         string                `json:"status"`
	PgbenchVersion string                `json:"pgbench_version"`
	Targets        map[string]TargetInfo `json:"targets"`
	ConnectionMode string                `json:"connection_mode"`
	Protocol       string                `json:"protocol"`
}

type Comparison struct {
	Version         int                    `json:"comparison_version"`
	DatasetID       string                 `json:"dataset_id"`
	Scenario        Scenario               `json:"scenario"`
	WorkloadVersion int                    `json:"workload_version"`
	WorkloadSHA256  string                 `json:"workload_sha256,omitempty"`
	SetupSHA256     string                 `json:"setup_sha256,omitempty"`
	ResetSHA256     string                 `json:"reset_sha256,omitempty"`
	LoadModel       string                 `json:"load_model"`
	StartingState   string                 `json:"starting_state"`
	Baseline        RunReference           `json:"baseline"`
	Candidate       RunReference           `json:"candidate"`
	Steps           []ComparedStep         `json:"steps"`
	Capacity        []ComparedCapacity     `json:"capacity"`
	RateCapacity    []ComparedRateCapacity `json:"capacity_arrival_rate,omitempty"`
	Warnings        []string               `json:"warnings"`
}

func ReadResult(path string) (*Result, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() > 4<<20 {
		return nil, fmt.Errorf("saved result must be a regular JSON file of at most 4 MiB")
	}
	contents, err := io.ReadAll(io.LimitReader(file, (4<<20)+1))
	if err != nil || len(contents) > 4<<20 {
		return nil, fmt.Errorf("cannot read saved result within the 4 MiB limit")
	}
	decoder := json.NewDecoder(bytes.NewReader(contents))
	decoder.DisallowUnknownFields()
	var result Result
	if err := decoder.Decode(&result); err != nil {
		return nil, fmt.Errorf("invalid saved result JSON; check its version, fields, and value types")
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return nil, fmt.Errorf("saved result must contain one JSON object")
	}
	var raw struct {
		Steps []struct {
			Measurement struct {
				Metrics map[string]json.RawMessage `json:"metrics"`
			} `json:"measurement"`
		} `json:"steps"`
	}
	if err := json.Unmarshal(contents, &raw); err != nil {
		return nil, fmt.Errorf("invalid saved measurement structure")
	}
	for _, step := range raw.Steps {
		required := []string{"completed_iterations", "failed_iterations", "successful_iteration_p95_ms", "error_rate"}
		if connectionMode(result.Scenario) == "persistent" {
			required = append(required, "tps_without_initial_connection")
		} else {
			required = append(required, "tps_including_connection_times", "pgbench_elapsed_seconds_including_connection_times", "average_connection_ms")
			if connectionMode(result.Scenario) == "burst" {
				required = append(required, "requested_iterations")
			}
		}
		if len(result.Scenario.ArrivalRates) > 0 {
			required = append(required, "requested_iterations", "skipped_iterations", "skipped_rate", "rate_attainment", "successful_iteration_schedule_lag_mean_ms", "successful_iteration_schedule_lag_max_ms")
		}
		for _, field := range required {
			value, present := step.Measurement.Metrics[field]
			if !present || string(value) == "null" {
				return nil, fmt.Errorf("saved measurement is missing required field %s", field)
			}
		}
	}
	if err := validateComparableResult(&result); err != nil {
		return nil, err
	}
	return &result, nil
}

func validateComparableResult(result *Result) error {
	if result.Version != 1 || result.Scenario.Version != 1 || result.WorkloadVersion != 1 || (result.Scenario.Workload != "customer-lookup" && result.Scenario.Workload != "custom-sql") || result.LoadModel != loadModel(result.Scenario) {
		return fmt.Errorf("comparison supports version-1 customer-lookup and custom-sql results")
	}
	if err := validateConnection(result.Scenario); err != nil {
		return err
	}
	if err := validateLoad(result.Scenario); err != nil {
		return err
	}
	if err := validateSavedState(result); err != nil {
		return err
	}
	custom := result.Scenario.Workload == "custom-sql"
	if custom {
		hash, err := hex.DecodeString(result.WorkloadSHA256)
		if err != nil || len(hash) != sha256.Size || result.WorkloadSHA256 != strings.ToLower(result.WorkloadSHA256) || result.Scenario.SQLFile == "" {
			return fmt.Errorf("custom SQL result requires sql_file and a valid workload SHA-256 identity")
		}
	} else if result.WorkloadSHA256 != "" || result.Scenario.SQLFile != "" {
		return fmt.Errorf("customer-lookup result contains custom SQL metadata")
	}
	if (result.Status != "completed" && result.Status != "failed") || result.StartedAt.IsZero() || result.FinishedAt == nil || result.FinishedAt.Before(result.StartedAt) || result.PgbenchVersion == "" {
		return fmt.Errorf("comparison requires finished experiments with recorded software versions")
	}
	if result.Scenario.Seed == nil || *result.Scenario.Seed > math.MaxInt64 || (!custom && result.Scenario.Schema == "") {
		return fmt.Errorf("saved result is missing dataset or workload identity metadata")
	}
	if custom && result.DatasetID == "" {
		if result.Scenario.Dataset != "" || !reflect.DeepEqual(result.DatasetIdentity, core.DatasetIdentity{}) {
			return fmt.Errorf("saved result has dataset metadata without dataset identity")
		}
	} else {
		if result.DatasetID == "" || result.DatasetIdentity.Model != "ecommerce" || result.DatasetIdentity.Counts["dim_customers"] < 1 {
			return fmt.Errorf("saved result is missing dataset identity metadata")
		}
		identity, err := json.Marshal(result.DatasetIdentity)
		if err != nil {
			return fmt.Errorf("invalid dataset identity metadata")
		}
		hash := sha256.Sum256(identity)
		if result.DatasetIdentity.SchemaVersion != core.SchemaVersion || result.DatasetID != hex.EncodeToString(hash[:]) {
			return fmt.Errorf("saved dataset identity does not match its metadata")
		}
	}
	warmup, warmupErr := time.ParseDuration(result.Scenario.Warmup)
	duration, durationErr := time.ParseDuration(result.Scenario.Duration)
	if warmupErr != nil || durationErr != nil || warmup < 0 || warmup > 5*time.Minute || duration < time.Second || duration > 5*time.Minute || warmup%time.Second != 0 || duration%time.Second != 0 {
		return fmt.Errorf("saved result has invalid phase durations")
	}
	limits := result.Scenario.Thresholds
	if !finite(limits.MaxP95MS) || limits.MaxP95MS <= 0 || !finite(limits.MaxErrorRate) || limits.MaxErrorRate < 0 || limits.MaxErrorRate > 1 || !finite(limits.MinTPS) || limits.MinTPS < 0 {
		return fmt.Errorf("saved result has invalid thresholds")
	}
	rateLoad := len(result.Scenario.ArrivalRates) > 0
	capacityCount := len(result.Capacity)
	if rateLoad {
		capacityCount = len(result.RateCapacity)
		if len(result.Capacity) != 0 {
			return fmt.Errorf("arrival-rate result contains client capacity")
		}
	} else if len(result.RateCapacity) != 0 {
		return fmt.Errorf("concurrency result contains arrival-rate capacity")
	}
	if len(result.Scenario.Targets) == 0 || len(result.Scenario.Targets) > 4 || len(result.Targets) != len(result.Scenario.Targets) || capacityCount != len(result.Scenario.Targets) {
		return fmt.Errorf("saved result is missing target, concurrency, or capacity metadata")
	}
	expected := make(map[stepKey]bool)
	for _, target := range result.Scenario.Targets {
		if target != "direct" && target != "shared" && target != "dedicated" && target != "libpq" {
			return fmt.Errorf("saved result contains an unsupported target")
		}
		info, present := result.Targets[target]
		if !present || info.Error != "" || info.ServerVersion == "" {
			return fmt.Errorf("cannot compare experiments with failed or missing target preflights")
		}
		if (result.Scenario.SetupFile != "" && !completedStatePhase(info.Setup)) || (result.Scenario.SetupFile == "" && info.Setup != nil) {
			return fmt.Errorf("saved result has incomplete or unexpected setup phases")
		}
		for _, level := range loadLevels(result.Scenario) {
			key := stepKey{target, level.clients, level.rate}
			if expected[key] {
				return fmt.Errorf("saved result has duplicate targets or invalid concurrency levels")
			}
			expected[key] = true
		}
	}
	if len(result.Steps) != len(expected) {
		return fmt.Errorf("cannot compare an incomplete load sweep")
	}
	capacity := make(map[string]float64)
	allPassed := true
	for _, step := range result.Steps {
		key := stepKey{step.Target, step.Concurrency, step.ArrivalRate}
		if !expected[key] {
			return fmt.Errorf("saved result contains duplicate or unplanned load steps")
		}
		delete(expected, key)
		if step.Measurement == nil || step.Measurement.Status != "completed" || step.Measurement.Error != "" || step.Measurement.Metrics == nil || (warmup > 0 && (step.Warmup == nil || step.Warmup.Status != "completed" || step.Warmup.Error != "")) || (warmup == 0 && step.Warmup != nil) {
			return fmt.Errorf("cannot compare incomplete or errored load phases")
		}
		metrics := step.Measurement.Metrics
		if metrics.CompletedIterations < 1 || metrics.FailedIterations < 0 || metrics.FailedIterations > math.MaxInt64-metrics.CompletedIterations || !finite(metrics.throughput()) || metrics.throughput() <= 0 || !finite(metrics.P95MS) || metrics.P95MS < 0 || !finite(metrics.ErrorRate) || metrics.ErrorRate < 0 || metrics.ErrorRate > 1 {
			return fmt.Errorf("saved result contains invalid measured values")
		}
		errorRate := float64(metrics.FailedIterations) / float64(metrics.CompletedIterations+metrics.FailedIterations)
		if math.Abs(errorRate-metrics.ErrorRate) > 1e-12 {
			return fmt.Errorf("saved error rate disagrees with iteration counts")
		}
		if err := validateConnectionMetrics(metrics, result.Scenario, step.Concurrency); err != nil {
			return err
		}
		if err := validateRateMetrics(metrics, step.ArrivalRate, connectionMode(result.Scenario)); err != nil {
			return err
		}
		failures := thresholdFailures(metrics, limits)
		status := "passed"
		if len(failures) > 0 {
			status = "failed"
			allPassed = false
		} else {
			level := float64(step.Concurrency)
			if rateLoad {
				level = step.ArrivalRate
			}
			if level > capacity[step.Target] {
				capacity[step.Target] = level
			}
		}
		if step.Status != status || !slices.Equal(step.Failures, failures) {
			return fmt.Errorf("saved load-step status disagrees with measured thresholds")
		}
	}
	for _, target := range result.Scenario.Targets {
		stored, present := result.Capacity[target]
		storedLevel := float64(stored)
		if rateLoad {
			storedLevel, present = result.RateCapacity[target]
		}
		if !present || storedLevel != capacity[target] {
			return fmt.Errorf("saved capacity disagrees with passing measured levels")
		}
	}
	if (result.Status == "completed") != allPassed {
		return fmt.Errorf("saved experiment status disagrees with its load steps")
	}
	return nil
}

type stepKey struct {
	target      string
	concurrency int
	rate        float64
}

func Compare(baseline, candidate *Result) (*Comparison, error) {
	for _, result := range []*Result{baseline, candidate} {
		if err := validateComparableResult(result); err != nil {
			return nil, err
		}
	}
	var incompatible []string
	for _, field := range []struct {
		name                string
		baseline, candidate any
	}{
		{"load model", baseline.LoadModel, candidate.LoadModel},
		{"arrival rates", baseline.Scenario.ArrivalRates, candidate.Scenario.ArrivalRates},
		{"rate client count", baseline.Scenario.Clients, candidate.Scenario.Clients},
		{"workload", baseline.Scenario.Workload, candidate.Scenario.Workload},
		{"workload SQL identity", baseline.WorkloadSHA256, candidate.WorkloadSHA256},
		{"access mode", accessMode(baseline.Scenario), accessMode(candidate.Scenario)},
		{"state policy", baseline.StartingState, candidate.StartingState},
		{"setup SQL identity", baseline.SetupSHA256, candidate.SetupSHA256},
		{"reset SQL identity", baseline.ResetSHA256, candidate.ResetSHA256},
		{"dataset identity", baseline.DatasetID, candidate.DatasetID},
		{"dataset metadata", baseline.DatasetIdentity, candidate.DatasetIdentity},
		{"schema", baseline.Scenario.Schema, candidate.Scenario.Schema},
		{"workload seed", *baseline.Scenario.Seed, *candidate.Scenario.Seed},
		{"target order", baseline.Scenario.Targets, candidate.Scenario.Targets},
		{"concurrency levels", baseline.Scenario.Concurrency, candidate.Scenario.Concurrency},
		{"thresholds", baseline.Scenario.Thresholds, candidate.Scenario.Thresholds},
	} {
		if !reflect.DeepEqual(field.baseline, field.candidate) {
			incompatible = append(incompatible, field.name)
		}
	}
	for _, field := range []struct{ name, baseline, candidate string }{
		{"latency limit", baseline.Scenario.LatencyLimit, candidate.Scenario.LatencyLimit},
		{"warm-up duration", baseline.Scenario.Warmup, candidate.Scenario.Warmup},
		{"measurement duration", baseline.Scenario.Duration, candidate.Scenario.Duration},
	} {
		before, _ := time.ParseDuration(field.baseline)
		after, _ := time.ParseDuration(field.candidate)
		if before != after {
			incompatible = append(incompatible, field.name)
		}
	}
	if len(incompatible) > 0 {
		return nil, fmt.Errorf("incompatible experiments: %s", strings.Join(incompatible, ", "))
	}
	reference := func(result *Result) RunReference {
		return RunReference{Name: result.Scenario.Name, StartedAt: result.StartedAt, Status: result.Status, PgbenchVersion: result.PgbenchVersion, Targets: result.Targets, ConnectionMode: connectionMode(result.Scenario), Protocol: result.Scenario.Protocol}
	}
	comparison := &Comparison{Version: 1, DatasetID: baseline.DatasetID, Scenario: baseline.Scenario, WorkloadVersion: baseline.WorkloadVersion, LoadModel: baseline.LoadModel, StartingState: baseline.StartingState, Baseline: reference(baseline), Candidate: reference(candidate), Warnings: []string{
		"Single-run differences do not establish statistical significance or a repeatable improvement.",
		"Target labels do not verify identical endpoints, hardware, pool sizing, or background traffic.",
	}}
	comparison.WorkloadSHA256 = baseline.WorkloadSHA256
	comparison.SetupSHA256 = baseline.SetupSHA256
	comparison.ResetSHA256 = baseline.ResetSHA256
	if baseline.Scenario.Protocol != candidate.Scenario.Protocol {
		comparison.Warnings = append(comparison.Warnings, "SQL protocols differ; this comparison measures an intentional protocol change.")
	}
	if connectionMode(baseline.Scenario) != connectionMode(candidate.Scenario) {
		comparison.Warnings = append(comparison.Warnings, "Connection modes differ; reconnect throughput includes connection setup. Unpaced transaction percentiles exclude each client's own connection setup, while rate-mode latency includes it as schedule lag.")
	}
	if baseline.StartingState == "reuse" {
		comparison.Warnings = append(comparison.Warnings, "State reuse carries writes across phases and targets; matching scripts do not prove equivalent starting data.")
	}
	if baseline.PgbenchVersion != candidate.PgbenchVersion {
		comparison.Warnings = append(comparison.Warnings, "pgbench versions differ; client behavior may affect measurements.")
	}
	byKey := make(map[stepKey]Step, len(candidate.Steps))
	for _, step := range candidate.Steps {
		byKey[stepKey{step.Target, step.Concurrency, step.ArrivalRate}] = step
	}
	for _, step := range baseline.Steps {
		other := byKey[stepKey{step.Target, step.Concurrency, step.ArrivalRate}]
		before, after := step.Measurement.Metrics, other.Measurement.Metrics
		comparison.Steps = append(comparison.Steps, ComparedStep{
			Target: step.Target, Concurrency: step.Concurrency, ArrivalRate: step.ArrivalRate, BaselineStatus: step.Status, CandidateStatus: other.Status,
			TPS: measuredChange(before.throughput(), after.throughput()), P95MS: measuredChange(before.P95MS, after.P95MS), ErrorRate: measuredChange(before.ErrorRate, after.ErrorRate),
			ErrorRatePercentagePoints: (after.ErrorRate - before.ErrorRate) * 100,
		})
		if step.ArrivalRate > 0 {
			skipped := measuredChange(*before.SkippedRate, *after.SkippedRate)
			attainment := measuredChange(*before.RateAttainment, *after.RateAttainment)
			comparison.Steps[len(comparison.Steps)-1].SkippedRate = &skipped
			comparison.Steps[len(comparison.Steps)-1].RateAttainment = &attainment
		}
	}
	for _, target := range baseline.Scenario.Targets {
		if len(baseline.Scenario.ArrivalRates) > 0 {
			before, after := baseline.RateCapacity[target], candidate.RateCapacity[target]
			comparison.RateCapacity = append(comparison.RateCapacity, ComparedRateCapacity{Target: target, Baseline: before, Candidate: after, Delta: after - before})
		} else {
			before, after := baseline.Capacity[target], candidate.Capacity[target]
			comparison.Capacity = append(comparison.Capacity, ComparedCapacity{Target: target, Baseline: before, Candidate: after, Delta: after - before})
		}
		if baseline.Targets[target].ServerVersion != candidate.Targets[target].ServerVersion {
			comparison.Warnings = append(comparison.Warnings, "PostgreSQL versions differ for target "+target+".")
		}
	}
	return comparison, nil
}

func measuredChange(baseline, candidate float64) Change {
	change := Change{Baseline: baseline, Candidate: candidate, Delta: candidate - baseline}
	if baseline != 0 {
		percent := change.Delta / baseline * 100
		if finite(percent) {
			change.Percent = &percent
		}
	}
	return change
}

func WriteComparison(path string, comparison *Comparison) error {
	contents, err := json.MarshalIndent(comparison, "", "  ")
	if err != nil {
		return err
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("comparison output must be a new file: %w", err)
	}
	if _, err := file.Write(append(contents, '\n')); err != nil {
		file.Close()
		os.Remove(path)
		return err
	}
	if err := file.Close(); err != nil {
		os.Remove(path)
		return err
	}
	return nil
}

func PrintComparison(writer io.Writer, comparison *Comparison) {
	fmt.Fprintf(writer, "Baseline: %s/%s; candidate: %s/%s\n", comparison.Baseline.ConnectionMode, comparison.Baseline.Protocol, comparison.Candidate.ConnectionMode, comparison.Candidate.Protocol)
	fmt.Fprintln(writer, "Observed changes: positive TPS is higher throughput; positive p95 is higher latency.")
	if len(comparison.Scenario.ArrivalRates) > 0 {
		fmt.Fprintln(writer, "Target      Clients  Rate/s  Base TPS   New TPS    TPS %   Base p95   New p95    p95 %   Error pp  Status")
	} else {
		fmt.Fprintln(writer, "Target      Clients  Base TPS   New TPS    TPS %   Base p95   New p95    p95 %   Error pp  Status")
	}
	percent := func(value *float64) string {
		if value == nil {
			return "n/a"
		}
		return fmt.Sprintf("%+.2f", *value)
	}
	for _, step := range comparison.Steps {
		fmt.Fprintf(writer, "%-11s %7d ", step.Target, step.Concurrency)
		if step.ArrivalRate > 0 {
			fmt.Fprintf(writer, "%7g ", step.ArrivalRate)
		}
		fmt.Fprintf(writer, "%9.2f %9.2f %8s %10.2f %9.2f %8s %10.3f  %s/%s\n", step.TPS.Baseline, step.TPS.Candidate, percent(step.TPS.Percent), step.P95MS.Baseline, step.P95MS.Candidate, percent(step.P95MS.Percent), step.ErrorRatePercentagePoints, step.BaselineStatus, step.CandidateStatus)
		if step.ArrivalRate > 0 {
			fmt.Fprintf(writer, "  Skipped rate: %.2f%% to %.2f%%; achieved rate: %.2f%% to %.2f%% of requested\n", step.SkippedRate.Baseline*100, step.SkippedRate.Candidate*100, step.RateAttainment.Baseline*100, step.RateAttainment.Candidate*100)
		}
	}
	fmt.Fprintln(writer, "p95 values are milliseconds; error changes are percentage points.")
	for _, capacity := range comparison.Capacity {
		if connectionMode(comparison.Scenario) == "burst" {
			fmt.Fprintf(writer, "%s highest passing tested burst size: %d baseline, %d candidate (%+d clients)\n", capacity.Target, capacity.Baseline, capacity.Candidate, capacity.Delta)
		} else {
			fmt.Fprintf(writer, "%s highest passing tested clients: %d baseline, %d candidate (%+d)\n", capacity.Target, capacity.Baseline, capacity.Candidate, capacity.Delta)
		}
	}
	for _, capacity := range comparison.RateCapacity {
		fmt.Fprintf(writer, "%s highest passing tested arrival rate: %g baseline, %g candidate (%+g iterations/s)\n", capacity.Target, capacity.Baseline, capacity.Candidate, capacity.Delta)
	}
	for _, warning := range comparison.Warnings {
		fmt.Fprintln(writer, "Warning:", warning)
	}
}
