package benchmark

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/peekknuf/Gengo/internal/core"
	"github.com/peekknuf/Gengo/internal/postgres"
)

type Phase struct {
	StartedAt      time.Time `json:"started_at,omitempty"`
	FinishedAt     time.Time `json:"finished_at,omitempty"`
	ElapsedSeconds float64   `json:"client_process_elapsed_seconds"`
	Status         string    `json:"status"`
	Error          string    `json:"error,omitempty"`
	Metrics        *Metrics  `json:"metrics,omitempty"`
}

type Step struct {
	Repetition             int      `json:"repetition,omitempty"`
	Target                 string   `json:"target"`
	Concurrency            int      `json:"concurrency"`
	ArrivalRate            float64  `json:"arrival_rate,omitempty"`
	Status                 string   `json:"status"`
	Warmup                 *Phase   `json:"warmup,omitempty"`
	Measurement            *Phase   `json:"measurement,omitempty"`
	ResetBeforeWarmup      *Phase   `json:"reset_before_warmup,omitempty"`
	ResetBeforeMeasurement *Phase   `json:"reset_before_measurement,omitempty"`
	ResetAfter             *Phase   `json:"reset_after_step,omitempty"`
	Failures               []string `json:"threshold_failures"`
}

type TargetInfo struct {
	ServerVersion string `json:"server_version,omitempty"`
	Error         string `json:"error,omitempty"`
	Setup         *Phase `json:"setup,omitempty"`
}

type Result struct {
	SupabaseMetrics *SupabaseTelemetry    `json:"supabase_metrics,omitempty"`
	Version         int                   `json:"result_version"`
	Status          string                `json:"status"`
	Error           string                `json:"error,omitempty"`
	StartedAt       time.Time             `json:"started_at"`
	FinishedAt      *time.Time            `json:"finished_at,omitempty"`
	Scenario        Scenario              `json:"scenario"`
	DatasetID       string                `json:"dataset_id"`
	DatasetIdentity core.DatasetIdentity  `json:"dataset_identity"`
	WorkloadVersion int                   `json:"workload_version"`
	WorkloadSHA256  string                `json:"workload_sha256,omitempty"`
	SetupSHA256     string                `json:"setup_sha256,omitempty"`
	ResetSHA256     string                `json:"reset_sha256,omitempty"`
	PgbenchVersion  string                `json:"pgbench_version"`
	LoadModel       string                `json:"load_model"`
	StartingState   string                `json:"starting_state"`
	Unavailable     []string              `json:"unavailable_measurements"`
	Targets         map[string]TargetInfo `json:"targets"`
	Steps           []Step                `json:"steps"`
	Capacity        map[string]int        `json:"capacity_clients"`
	RateCapacity    map[string]float64    `json:"capacity_arrival_rate,omitempty"`
	Schedule        []Round               `json:"schedule,omitempty"`
	Aggregates      []Aggregate           `json:"aggregates,omitempty"`
}

func Run(ctx context.Context, plan Plan, envFile, output string, progress io.Writer) (*Result, error) {
	clients := make(map[string]*postgres.Client)
	for _, target := range plan.Scenario.Targets {
		client, err := postgres.NewClient(target, envFile)
		if err != nil {
			return nil, err
		}
		clients[target] = client
	}
	output, err := filepath.Abs(output)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Dir(output), 0755); err != nil {
		return nil, err
	}
	if err := os.Mkdir(output, 0755); err != nil {
		return nil, fmt.Errorf("benchmark output must be a new directory: %w", err)
	}
	result := &Result{
		Version: 1, Status: "running", StartedAt: time.Now().UTC(), Scenario: plan.Scenario,
		DatasetID: plan.Dataset.DatasetID, DatasetIdentity: plan.Dataset.Identity, WorkloadVersion: 1, LoadModel: loadModel(plan.Scenario),
		StartingState: statePolicy(plan.Scenario), SetupSHA256: sqlHash(plan.SetupSQL), ResetSHA256: sqlHash(plan.ResetSQL), Targets: make(map[string]TargetInfo), Capacity: make(map[string]int), Steps: []Step{},
		Unavailable: []string{"requested_iteration_count_for_duration_load", "separate_timeout_count", "server_query_latency", "pooler_wait", "client_queue", "server_and_pooler_telemetry", "dataset_generation_and_import_timings"},
	}
	result.Schedule = roundSchedule(plan.Scenario)
	if len(plan.Scenario.ArrivalRates) > 0 {
		result.Capacity = nil
		result.RateCapacity = make(map[string]float64)
		result.Unavailable = []string{"separate_timeout_count", "server_query_latency", "pooler_wait", "server_and_pooler_telemetry", "dataset_generation_and_import_timings"}
	}
	if connectionMode(plan.Scenario) != "persistent" {
		result.Unavailable = append(result.Unavailable, "connection_setup_latency_percentiles", "separate_initial_connection_time")
		if len(plan.Scenario.ArrivalRates) == 0 {
			result.Unavailable = append(result.Unavailable, "connection_start_to_completion_latency_percentiles")
		}
	}
	stopMetrics := startMetrics(ctx, plan.Scenario, envFile, output)
	defer stopMetrics()
	finish := func(runErr error) (*Result, error) {
		result.SupabaseMetrics = stopMetrics()
		if result.SupabaseMetrics.Status == "available" || result.SupabaseMetrics.Status == "partial" {
			for i, measurement := range result.Unavailable {
				if measurement == "server_and_pooler_telemetry" {
					result.Unavailable[i] = "additional_server_and_pooler_telemetry"
				}
			}
		} else {
			result.Unavailable = append(result.Unavailable, "supabase_metrics")
		}
		finished := time.Now().UTC()
		result.FinishedAt = &finished
		if runErr != nil {
			result.Status = "failed"
			result.Error = runErr.Error()
			if ctx.Err() != nil {
				result.Status = "canceled"
			}
		} else {
			result.Status = "completed"
		}
		if err := saveResult(output, result); err != nil {
			return nil, fmt.Errorf("save benchmark results: %w", err)
		}
		return result, runErr
	}
	workload := "\\set customer_id random(1, :customer_count)\nSELECT customer_id, first_name, last_name, email FROM \"" + plan.Scenario.Schema + "\".dim_customers WHERE customer_id = :customer_id;\n"
	if plan.Scenario.Workload == "custom-sql" {
		if plan.SQL == "" {
			return finish(fmt.Errorf("custom SQL plan has no script; load the scenario before running"))
		}
		workload = plan.SQL
		result.WorkloadSHA256 = sqlHash(workload)
	}
	if err := saveResult(output, result); err != nil {
		return finish(err)
	}
	script := filepath.Join(output, "workload.sql")
	if err := os.WriteFile(script, []byte(workload), 0644); err != nil {
		return finish(err)
	}
	for _, stateScript := range []struct{ name, sql string }{{"setup.sql", plan.SetupSQL}, {"reset.sql", plan.ResetSQL}} {
		if stateScript.sql != "" {
			if err := os.WriteFile(filepath.Join(output, stateScript.name), []byte(stateScript.sql), 0644); err != nil {
				return finish(err)
			}
		}
	}
	var version, diagnostics bytes.Buffer
	versionContext, versionCancel := context.WithTimeout(ctx, 30*time.Second)
	err = clients[plan.Scenario.Targets[0]].RunPgbench(versionContext, []string{"--version"}, &version, &diagnostics)
	versionCancel()
	if err != nil {
		return finish(fmt.Errorf("pgbench version check: %w; %s", err, strings.TrimSpace(diagnostics.String())))
	}
	result.PgbenchVersion = strings.TrimSpace(version.String())
	versionMatch := regexp.MustCompile(`^pgbench \(PostgreSQL\) ([0-9]+)\.`).FindStringSubmatch(result.PgbenchVersion)
	if len(versionMatch) != 2 {
		return finish(fmt.Errorf("unrecognized pgbench version output"))
	}
	majorVersion, err := strconv.Atoi(versionMatch[1])
	if err != nil || majorVersion < 17 {
		return finish(fmt.Errorf("pgbench 17 or later is required"))
	}
	preflightFailed := false
	for _, round := range result.Schedule {
		for _, target := range round.Targets {
			client := clients[target]
			if existing, checked := result.Targets[target]; checked {
				if existing.Error != "" {
					continue
				}
			} else {
				serverVersion, err := checkDataset(ctx, client, plan)
				if err != nil {
					result.Targets[target] = TargetInfo{Error: err.Error()}
					preflightFailed = true
					if ctx.Err() != nil {
						return finish(ctx.Err())
					}
					if err := saveResult(output, result); err != nil {
						return finish(err)
					}
					continue
				}
				result.Targets[target] = TargetInfo{ServerVersion: serverVersion}
				if result.RateCapacity != nil {
					result.RateCapacity[target] = 0
				} else {
					result.Capacity[target] = 0
				}
				if plan.SetupSQL != "" {
					setup := runStateScript(ctx, client, filepath.Join(output, "setup.sql"), filepath.Join(output, target+"-setup"))
					result.Targets[target] = TargetInfo{ServerVersion: serverVersion, Setup: setup}
					if setup.Status != "completed" {
						return finish(fmt.Errorf("%s setup failed: %s", target, setup.Error))
					}
					if err := saveResult(output, result); err != nil {
						return finish(err)
					}
				}
			}
			for _, level := range loadLevels(plan.Scenario) {
				concurrency := level.clients
				stepPrefix := target + "-" + level.label()
				if repetitions(plan.Scenario) > 1 {
					stepPrefix = fmt.Sprintf("rep-%d-%s", round.Repetition, stepPrefix)
				}
				if ctx.Err() != nil {
					return finish(ctx.Err())
				}
				step := Step{Repetition: round.Repetition, Target: target, Concurrency: concurrency, ArrivalRate: level.rate, Status: "running", Failures: []string{}}
				result.Steps = append(result.Steps, step)
				current := &result.Steps[len(result.Steps)-1]
				if err := saveResult(output, result); err != nil {
					return finish(err)
				}
				fmt.Fprintf(progress, "%s: %d clients\n", target, concurrency)
				if repetitions(plan.Scenario) > 1 {
					fmt.Fprintf(progress, "Repetition %d/%d\n", round.Repetition, repetitions(plan.Scenario))
				}
				if level.rate > 0 {
					fmt.Fprintf(progress, "Requested arrival rate: %g iterations/s\n", level.rate)
				}
				reset := func(phase **Phase, label string) error {
					if statePolicy(plan.Scenario) != "reset-per-phase" {
						return nil
					}
					*phase = runStateScript(ctx, client, filepath.Join(output, "reset.sql"), filepath.Join(output, stepPrefix+"-"+label))
					if (*phase).Status != "completed" {
						current.Status = "error"
						return fmt.Errorf("%s %d clients %s failed: %s", target, concurrency, label, (*phase).Error)
					}
					return saveResult(output, result)
				}
				if plan.Warmup > 0 {
					if err := reset(&current.ResetBeforeWarmup, "reset-before-warmup"); err != nil {
						return finish(err)
					}
					current.Warmup = runPhase(ctx, client, plan, level, plan.Warmup, script, filepath.Join(output, stepPrefix+"-warmup"), false)
					if err := saveResult(output, result); err != nil {
						return finish(err)
					}
				}
				if current.Warmup == nil || current.Warmup.Status == "completed" {
					if err := reset(&current.ResetBeforeMeasurement, "reset-before-measurement"); err != nil {
						return finish(err)
					}
					current.Measurement = runPhase(ctx, client, plan, level, plan.Duration, script, filepath.Join(output, stepPrefix+"-measurement"), true)
				}
				if ctx.Err() != nil {
					current.Status = "canceled"
					return finish(ctx.Err())
				}
				if err := reset(&current.ResetAfter, "reset-after-step"); err != nil {
					return finish(err)
				}
				current.Status = "error"
				if current.Measurement != nil && current.Measurement.Status == "completed" {
					current.Failures = thresholdFailures(current.Measurement.Metrics, plan.Scenario.Thresholds)
					current.Status = "passed"
					if len(current.Failures) != 0 {
						current.Status = "failed"
					}
				}
				if ctx.Err() != nil {
					current.Status = "canceled"
					return finish(ctx.Err())
				}
				if err := saveResult(output, result); err != nil {
					return finish(err)
				}
			}
		}
	}
	refreshAggregates(result)
	qualified := !preflightFailed
	for _, aggregate := range result.Aggregates {
		if aggregate.Status != "passed" {
			qualified = false
		}
	}
	if !qualified {
		return finish(fmt.Errorf("one or more benchmark targets or load steps failed; inspect result.json"))
	}
	return finish(nil)
}

func checkDataset(ctx context.Context, client *postgres.Client, plan Plan) (string, error) {
	checkContext, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	var output, diagnostics bytes.Buffer
	if plan.Scenario.Workload == "custom-sql" {
		if err := client.Run(checkContext, []string{"-At", "--command", "SELECT current_setting('server_version');"}, &output, &diagnostics); err != nil {
			return "", fmt.Errorf("connection preflight failed: %w; %s", err, strings.TrimSpace(diagnostics.String()))
		}
		version := strings.TrimSpace(output.String())
		if version == "" {
			return "", fmt.Errorf("connection preflight returned no server version")
		}
		return version, nil
	}
	query := `SELECT current_setting('server_version'), count(*), min(customer_id), max(customer_id), count(DISTINCT customer_id) FROM "` + plan.Scenario.Schema + `".dim_customers;`
	if err := client.Run(checkContext, []string{"-At", "--command", query}, &output, &diagnostics); err != nil {
		return "", fmt.Errorf("dataset preflight failed: %w; %s", err, strings.TrimSpace(diagnostics.String()))
	}
	fields := strings.Split(strings.TrimSpace(output.String()), "|")
	count := strconv.FormatInt(plan.Dataset.Identity.Counts["dim_customers"], 10)
	if len(fields) != 5 || fields[1] != count || fields[2] != "1" || fields[3] != count || fields[4] != count {
		return "", fmt.Errorf("database customer keys do not match the manifest's contiguous 1..N range")
	}
	return fields[0], nil
}

func runPhase(ctx context.Context, client *postgres.Client, plan Plan, level loadLevel, duration time.Duration, script, directory string, measured bool) *Phase {
	phase := &Phase{Status: "error", StartedAt: time.Now().UTC()}
	defer func() { phase.FinishedAt = time.Now().UTC() }()
	if err := os.Mkdir(directory, 0755); err != nil {
		phase.Error = err.Error()
		return phase
	}
	mode := connectionMode(plan.Scenario)
	arguments := []string{"--no-vacuum", "--exit-on-abort", "--max-tries=1", "--protocol=" + plan.Scenario.Protocol, "--client=" + strconv.Itoa(level.clients), "--random-seed=" + strconv.FormatUint(*plan.Scenario.Seed, 10), "--file=" + script + "@1"}
	if mode == "burst" {
		arguments = append(arguments, "--connect", "--jobs="+strconv.Itoa(level.clients), "--transactions=1")
	} else {
		arguments = append(arguments, "--jobs=1", "--time="+strconv.Itoa(int(duration/time.Second)))
		if mode == "reconnect" {
			arguments = append(arguments, "--connect")
		}
	}
	if level.rate > 0 {
		limit, _ := time.ParseDuration(plan.Scenario.LatencyLimit)
		arguments = append(arguments, "--rate="+strconv.FormatFloat(level.rate, 'f', -1, 64), "--latency-limit="+strconv.FormatFloat(float64(limit)/float64(time.Millisecond), 'f', -1, 64))
	}
	if plan.Dataset.DatasetID != "" {
		arguments = append(arguments, "--define=customer_count="+strconv.FormatInt(plan.Dataset.Identity.Counts["dim_customers"], 10))
	}
	prefix := filepath.Join(directory, "transactions")
	if measured {
		arguments = append(arguments, "--log", "--log-prefix="+prefix)
	}
	timeout := duration + 30*time.Second
	if mode == "burst" {
		timeout = duration
	}
	phaseContext, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	var output, diagnostics bytes.Buffer
	start := time.Now()
	err := client.RunPgbench(phaseContext, arguments, &output, &diagnostics)
	phase.ElapsedSeconds = time.Since(start).Seconds()
	if writeErr := os.WriteFile(filepath.Join(directory, "pgbench.txt"), []byte(output.String()+"\n"+diagnostics.String()), 0644); writeErr != nil {
		phase.Error = writeErr.Error()
		return phase
	}
	if err != nil {
		phase.Error = err.Error() + "; " + strings.TrimSpace(diagnostics.String())
		return phase
	}
	if measured {
		phase.Metrics, err = readMetrics(output.String(), prefix, level.rate, mode)
		if err != nil {
			phase.Error = err.Error()
			return phase
		}
		if mode == "burst" {
			requested := int64(level.clients)
			phase.Metrics.RequestedIterations = &requested
		}
		if err := validateConnectionMetrics(phase.Metrics, plan.Scenario, level.clients); err != nil {
			phase.Error = err.Error()
			return phase
		}
	}
	phase.Status = "completed"
	return phase
}

func saveResult(directory string, result *Result) error {
	refreshAggregates(result)
	contents, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return err
	}
	file, err := os.CreateTemp(directory, ".result-*.json")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	if _, err := file.Write(append(contents, '\n')); err != nil {
		file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	return os.Rename(file.Name(), filepath.Join(directory, "result.json"))
}

func PrintSummary(writer io.Writer, result *Result) {
	if result.Scenario.Profile != "" {
		fmt.Fprintf(writer, "Profile: %s (version %d); warm-up %s; measurement %s; repetitions %d; target order %s\n", result.Scenario.Profile, result.Scenario.ProfileVersion, result.Scenario.Warmup, result.Scenario.Duration, repetitions(result.Scenario), targetOrder(result.Scenario))
	}
	if telemetry := result.SupabaseMetrics; telemetry != nil {
		fmt.Fprintf(writer, "Supabase metrics: %s; %d scrape attempts; interval %ds; scope %s\n", telemetry.Status, len(telemetry.Samples), telemetry.IntervalSeconds, telemetry.Scope)
		if telemetry.Reason != "" {
			fmt.Fprintf(writer, "  %s\n", telemetry.Reason)
		}
	}
	fmt.Fprintf(writer, "Workload: %s; access: %s; state policy: %s\n", result.Scenario.Workload, accessMode(result.Scenario), result.StartingState)
	fmt.Fprintf(writer, "Load model: %s\n", result.LoadModel)
	fmt.Fprintf(writer, "Connection mode: %s; protocol: %s\n", connectionMode(result.Scenario), result.Scenario.Protocol)
	if connectionMode(result.Scenario) != "persistent" {
		if len(result.Scenario.ArrivalRates) > 0 {
			fmt.Fprintln(writer, "TPS and scheduled-start p95 include connection time.")
		} else {
			fmt.Fprintln(writer, "TPS includes connection time; p95 excludes each client's own connection setup.")
		}
	}
	if repetitions(result.Scenario) > 1 {
		printRepeatedSummary(writer, result)
	} else {
		if len(result.Scenario.ArrivalRates) > 0 {
			fmt.Fprintln(writer, "Target      Clients  Rate/s  Status       TPS      p95 ms  Errors  Skipped  Achieved %")
		} else {
			fmt.Fprintln(writer, "Target      Clients  Status       TPS      p95 ms  Errors")
		}
		for _, step := range result.Steps {
			if step.Measurement != nil && step.Measurement.Metrics != nil {
				metrics := step.Measurement.Metrics
				if step.ArrivalRate > 0 {
					fmt.Fprintf(writer, "%-11s %7d %7g  %-9s %8.2f %9.2f %7d %8d %10.2f\n", step.Target, step.Concurrency, step.ArrivalRate, step.Status, metrics.throughput(), metrics.P95MS, metrics.FailedIterations, metrics.SkippedIterations, *metrics.RateAttainment*100)
				} else {
					fmt.Fprintf(writer, "%-11s %7d  %-9s %8.2f %9.2f %7d\n", step.Target, step.Concurrency, step.Status, metrics.throughput(), metrics.P95MS, metrics.FailedIterations)
				}
				if metrics.ConnectionMeanMS != nil {
					fmt.Fprintf(writer, "  Mean connection and disconnection: %.2f ms\n", *metrics.ConnectionMeanMS)
				}
			} else {
				fmt.Fprintf(writer, "%-11s %7d  ", step.Target, step.Concurrency)
				if step.ArrivalRate > 0 {
					fmt.Fprintf(writer, "%7g  ", step.ArrivalRate)
				}
				fmt.Fprintf(writer, "%-9s        -         -       -\n", step.Status)
			}
		}
	}
	for _, target := range result.Scenario.Targets {
		info, ok := result.Targets[target]
		if !ok {
			fmt.Fprintf(writer, "%s: not attempted\n", target)
			continue
		}
		if info.Error != "" {
			fmt.Fprintf(writer, "%s preflight: %s\n", target, info.Error)
			continue
		}
		if len(result.Scenario.ArrivalRates) > 0 {
			fmt.Fprintf(writer, "%s highest passing tested arrival rate: %g iterations/s\n", target, result.RateCapacity[target])
		} else if connectionMode(result.Scenario) == "burst" {
			fmt.Fprintf(writer, "%s highest passing tested burst size: %d clients\n", target, result.Capacity[target])
		} else {
			fmt.Fprintf(writer, "%s highest passing tested concurrency: %d\n", target, result.Capacity[target])
		}
	}
}
