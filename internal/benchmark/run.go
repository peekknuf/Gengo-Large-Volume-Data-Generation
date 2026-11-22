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
	ElapsedSeconds float64  `json:"client_process_elapsed_seconds"`
	Status         string   `json:"status"`
	Error          string   `json:"error,omitempty"`
	Metrics        *Metrics `json:"metrics,omitempty"`
}

type Step struct {
	Target      string   `json:"target"`
	Concurrency int      `json:"concurrency"`
	Status      string   `json:"status"`
	Warmup      *Phase   `json:"warmup,omitempty"`
	Measurement *Phase   `json:"measurement,omitempty"`
	Failures    []string `json:"threshold_failures"`
}

type TargetInfo struct {
	ServerVersion string `json:"server_version,omitempty"`
	Error         string `json:"error,omitempty"`
}

type Result struct {
	Version         int                   `json:"result_version"`
	Status          string                `json:"status"`
	Error           string                `json:"error,omitempty"`
	StartedAt       time.Time             `json:"started_at"`
	FinishedAt      *time.Time            `json:"finished_at,omitempty"`
	Scenario        Scenario              `json:"scenario"`
	DatasetID       string                `json:"dataset_id"`
	DatasetIdentity core.DatasetIdentity  `json:"dataset_identity"`
	WorkloadVersion int                   `json:"workload_version"`
	PgbenchVersion  string                `json:"pgbench_version"`
	LoadModel       string                `json:"load_model"`
	StartingState   string                `json:"starting_state"`
	Unavailable     []string              `json:"unavailable_measurements"`
	Targets         map[string]TargetInfo `json:"targets"`
	Steps           []Step                `json:"steps"`
	Capacity        map[string]int        `json:"capacity_clients"`
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
		DatasetID: plan.Dataset.DatasetID, DatasetIdentity: plan.Dataset.Identity, WorkloadVersion: 1, LoadModel: "closed-loop-concurrency",
		StartingState: "read-only-reuse", Targets: make(map[string]TargetInfo), Capacity: make(map[string]int), Steps: []Step{},
		Unavailable: []string{"requested_iteration_count_for_duration_load", "separate_timeout_count", "server_query_latency", "pooler_wait", "client_queue", "server_and_pooler_telemetry", "dataset_generation_and_import_timings"},
	}
	finish := func(runErr error) (*Result, error) {
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
	if err := saveResult(output, result); err != nil {
		return nil, err
	}
	workload := "\\set customer_id random(1, :customer_count)\nSELECT customer_id, first_name, last_name, email FROM \"" + plan.Scenario.Schema + "\".dim_customers WHERE customer_id = :customer_id;\n"
	script := filepath.Join(output, "workload.sql")
	if err := os.WriteFile(script, []byte(workload), 0644); err != nil {
		return finish(err)
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
	allPassed := true
	for _, target := range plan.Scenario.Targets {
		client := clients[target]
		serverVersion, err := checkDataset(ctx, client, plan)
		if err != nil {
			result.Targets[target] = TargetInfo{Error: err.Error()}
			allPassed = false
			if ctx.Err() != nil {
				return finish(ctx.Err())
			}
			if err := saveResult(output, result); err != nil {
				return finish(err)
			}
			continue
		}
		result.Targets[target] = TargetInfo{ServerVersion: serverVersion}
		result.Capacity[target] = 0
		for _, concurrency := range plan.Scenario.Concurrency {
			if ctx.Err() != nil {
				return finish(ctx.Err())
			}
			step := Step{Target: target, Concurrency: concurrency, Status: "running", Failures: []string{}}
			result.Steps = append(result.Steps, step)
			current := &result.Steps[len(result.Steps)-1]
			if err := saveResult(output, result); err != nil {
				return finish(err)
			}
			fmt.Fprintf(progress, "%s: %d clients\n", target, concurrency)
			if plan.Warmup > 0 {
				current.Warmup = runPhase(ctx, client, plan, concurrency, plan.Warmup, script, filepath.Join(output, fmt.Sprintf("%s-c%d-warmup", target, concurrency)), false)
				if err := saveResult(output, result); err != nil {
					return finish(err)
				}
			}
			if current.Warmup == nil || current.Warmup.Status == "completed" {
				current.Measurement = runPhase(ctx, client, plan, concurrency, plan.Duration, script, filepath.Join(output, fmt.Sprintf("%s-c%d-measurement", target, concurrency)), true)
			}
			current.Status = "error"
			if current.Measurement != nil && current.Measurement.Status == "completed" {
				current.Failures = thresholdFailures(current.Measurement.Metrics, plan.Scenario.Thresholds)
				current.Status = "passed"
				if len(current.Failures) != 0 {
					current.Status = "failed"
				} else {
					result.Capacity[target] = concurrency
				}
			}
			if current.Status != "passed" {
				allPassed = false
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
	if !allPassed {
		return finish(fmt.Errorf("one or more benchmark targets or load steps failed; inspect result.json"))
	}
	return finish(nil)
}

func checkDataset(ctx context.Context, client *postgres.Client, plan Plan) (string, error) {
	checkContext, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	var output, diagnostics bytes.Buffer
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

func runPhase(ctx context.Context, client *postgres.Client, plan Plan, concurrency int, duration time.Duration, script, directory string, measured bool) *Phase {
	phase := &Phase{Status: "error"}
	if err := os.Mkdir(directory, 0755); err != nil {
		phase.Error = err.Error()
		return phase
	}
	arguments := []string{"--no-vacuum", "--exit-on-abort", "--max-tries=1", "--protocol=simple", "--jobs=1", "--client=" + strconv.Itoa(concurrency), "--time=" + strconv.Itoa(int(duration/time.Second)), "--random-seed=" + strconv.FormatUint(*plan.Scenario.Seed, 10), "--define=customer_count=" + strconv.FormatInt(plan.Dataset.Identity.Counts["dim_customers"], 10), "--file=" + script + "@1"}
	prefix := filepath.Join(directory, "transactions")
	if measured {
		arguments = append(arguments, "--log", "--log-prefix="+prefix)
	}
	phaseContext, cancel := context.WithTimeout(ctx, duration+30*time.Second)
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
		phase.Metrics, err = readMetrics(output.String(), prefix)
		if err != nil {
			phase.Error = err.Error()
			return phase
		}
	}
	phase.Status = "completed"
	return phase
}

func saveResult(directory string, result *Result) error {
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
	fmt.Fprintln(writer, "Target      Clients  Status       TPS      p95 ms  Errors")
	for _, step := range result.Steps {
		if step.Measurement != nil && step.Measurement.Metrics != nil {
			metrics := step.Measurement.Metrics
			fmt.Fprintf(writer, "%-11s %7d  %-9s %8.2f %9.2f %7d\n", step.Target, step.Concurrency, step.Status, metrics.TPS, metrics.P95MS, metrics.FailedIterations)
		} else {
			fmt.Fprintf(writer, "%-11s %7d  %-9s        -         -       -\n", step.Target, step.Concurrency, step.Status)
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
		fmt.Fprintf(writer, "%s highest passing tested concurrency: %d\n", target, result.Capacity[target])
	}
}
