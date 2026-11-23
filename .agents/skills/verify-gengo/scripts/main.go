package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/joho/godotenv"
	"github.com/peekknuf/Gengo/internal/benchmark"
	"github.com/peekknuf/Gengo/internal/core"
	"github.com/peekknuf/Gengo/internal/postgres"
)

type verification struct {
	Evidence         string   `json:"evidence"`
	Schema           string   `json:"schema"`
	Targets          []string `json:"targets"`
	Status           string   `json:"status"`
	Database         string   `json:"database"`
	CleanupVerified  bool     `json:"cleanup_verified"`
	MetricsVerified  bool     `json:"metrics_verified"`
	ProfilesVerified bool     `json:"profiles_verified"`
	Error            string   `json:"error,omitempty"`
	ctx              context.Context
	binary           string
	envFile          string
	requireMetrics   bool
	profiles         bool
}

func main() {
	envFile := flag.String("env-file", ".env", "Dotenv path, never credential contents")
	targets := flag.String("targets", "direct", "Comma-separated benchmark targets; import uses direct")
	evidence := flag.String("evidence", "", "New evidence directory; default is a unique OS temporary directory")
	offline := flag.Bool("offline", false, "Explicitly skip database checks and workloads")
	doctor := flag.Bool("doctor", false, "Build and check tools/connections without tests or mutations")
	requireMetrics := flag.Bool("require-metrics", false, "Require live Supabase metrics snapshots and verify their files")
	profiles := flag.Bool("profiles", false, "Run full quick and thorough profiles on a seeded 1000-customer fixture")
	reportProfiles := flag.String("report-profiles", "", "Create an HTML/JSON report from completed profile results in an evidence directory")
	flag.Parse()
	if *reportProfiles != "" {
		if err := writeProfileReport(*reportProfiles); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}
	if err := verify(*envFile, *targets, *evidence, *offline, *doctor, *requireMetrics, *profiles); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func verify(envFile, targets, evidence string, offline, doctor, requireMetrics, profiles bool) (err error) {
	if (requireMetrics || profiles) && (offline || doctor) {
		return fmt.Errorf("-require-metrics and -profiles require live workload verification")
	}
	module, err := os.ReadFile("go.mod")
	if err != nil || !strings.Contains(string(module), "module github.com/peekknuf/Gengo") {
		return fmt.Errorf("run from the Gengo repository root")
	}
	if evidence == "" {
		evidence, err = os.MkdirTemp("", "gengo-verify-")
	} else {
		evidence, err = filepath.Abs(evidence)
		if err == nil {
			err = os.Mkdir(evidence, 0700)
		}
	}
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, 20*time.Minute)
	defer cancel()
	v := &verification{Evidence: evidence, Targets: strings.Split(targets, ","), Status: "running", Database: "skipped", ctx: ctx, envFile: envFile, requireMetrics: requireMetrics, profiles: profiles}
	for _, target := range v.Targets {
		if target != "direct" && target != "shared" && target != "dedicated" {
			return fmt.Errorf("targets must be direct,shared,dedicated")
		}
	}
	fmt.Println("Evidence:", evidence)
	defer func() {
		if failure := recover(); failure != nil {
			err = errors.Join(err, fmt.Errorf("verification panicked: %v", failure))
		}
		if v.binary != "" {
			removeErr := os.Remove(v.binary)
			if !os.IsNotExist(removeErr) {
				err = errors.Join(err, removeErr)
			}
		}
		v.Status = "passed"
		if err != nil {
			v.Status, v.Error = "failed", err.Error()
		}
		err = errors.Join(err, v.save("summary.json", v))
		_, evidenceErr := os.Stat(filepath.Join(evidence, "summary.json"))
		err = errors.Join(err, evidenceErr)
	}()
	if _, err = v.command("go-version", nil, "go", "version"); err != nil {
		return err
	}
	if !doctor {
		env := []string{"GENGO_POSTGRES_TEST_ENV_FILE=", "GENGO_KEEP_OUTPUT=0", "GENGO_TEST_SIZE_GB=0.0001"}
		if _, err = v.command("go-test", env, "go", "test", "./...", "-count=1", "-timeout=10m"); err != nil {
			return err
		}
		if _, err = v.command("go-vet", nil, "go", "vet", "./..."); err != nil {
			return err
		}
	}
	name := "gengo"
	if runtime.GOOS == "windows" {
		name += ".exe"
	}
	v.binary = filepath.Join(evidence, name)
	if _, err = v.command("go-build", nil, "go", "build", "-o", v.binary, "."); err != nil {
		return err
	}
	if _, err = v.cli("cli-help", "--help"); err != nil {
		return err
	}
	var client *postgres.Client
	if !offline {
		if _, err = v.command("psql-version", nil, "psql", "--version"); err != nil {
			return err
		}
		if _, err = v.command("pgbench-version", nil, "pgbench", "--version"); err != nil {
			return err
		}
		for _, target := range v.Targets {
			if _, err = v.cli("doctor-"+target, "db", "check", "--target", target, "--env-file", envFile, "--timeout", "20s"); err != nil {
				return err
			}
		}
		client, err = postgres.NewClient("direct", envFile)
		if err != nil {
			return err
		}
		if _, err = v.query(ctx, client, "doctor-query", "SELECT 1;"); err != nil {
			return err
		}
		v.Database = "connected"
	}
	if doctor {
		return nil
	}
	id := make([]byte, 10)
	if _, err = rand.Read(id); err != nil {
		return err
	}
	v.Schema = "gengo_verify_" + hex.EncodeToString(id)
	if err = v.save("summary.json", v); err != nil {
		return err
	}
	dataset := filepath.Join(evidence, "dataset")
	args := []string{"gen", "--model", "ecommerce", "--size", "0.0001", "--format", "csv", "--seed", "0", "--as-of", "2026-11-21", "--workers", "2", "--rows-per-file", "4", "--output", dataset}
	customerCount := 3
	addressCount := 5
	if profiles {
		customerCount = 1000
		addressCount = 1000
	}
	for _, row := range strings.Fields(fmt.Sprintf("customers=%d customer_addresses=%d suppliers=2 products=4 product_categories=10 order_headers=7 order_items=39", customerCount, addressCount)) {
		args = append(args, "--rows", row)
	}
	if _, err = v.cli("dry-run", append(args, "--dry-run")...); err != nil {
		return err
	}
	if _, err = os.Stat(dataset); !os.IsNotExist(err) {
		return fmt.Errorf("dry-run created output or output cannot be inspected: %v", err)
	}
	if _, err = v.cli("generate", args...); err != nil {
		return err
	}
	var manifest core.Manifest
	if err = readJSON(filepath.Join(dataset, "manifest.json"), &manifest); err != nil {
		return err
	}
	if !manifest.Complete || manifest.DatasetID == "" || manifest.Identity.Seed != 0 || manifest.Identity.AsOf != "2026-11-21" || manifest.Identity.RowsPerFile != 4 {
		return fmt.Errorf("generated manifest lost explicit configuration")
	}
	if err = verifyCSV(dataset, manifest); err != nil {
		return err
	}
	if err = v.save("generation-proof.json", manifest); err != nil {
		return err
	}
	if _, err = v.cli("load-plan", "load", "plan", "--dataset", dataset, "--schema", v.Schema, "--output", filepath.Join(evidence, "load-plan")); err != nil {
		return err
	}
	for _, name := range []string{"run.psql", "schema.sql", "load.sql", "indexes.sql", "foreign_keys.sql", "validate_foreign_keys.sql", "verify.sql", "analyze.sql"} {
		if _, err = os.Stat(filepath.Join(evidence, "load-plan", name)); err != nil {
			return err
		}
	}
	if offline {
		return nil
	}
	exists, err := v.query(ctx, client, "schema-preflight", "SELECT count(*) FROM pg_namespace WHERE nspname='"+v.Schema+"';")
	if err != nil {
		return err
	}
	if exists != "0" {
		return fmt.Errorf("refusing to use an existing schema")
	}
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		_, cleanupErr := v.query(cleanupCtx, client, "cleanup-drop", "DROP SCHEMA IF EXISTS "+v.Schema+" CASCADE;")
		if cleanupErr == nil {
			var count string
			count, cleanupErr = v.query(cleanupCtx, client, "cleanup-check", "SELECT count(*) FROM pg_namespace WHERE nspname='"+v.Schema+"';")
			if cleanupErr == nil && count != "0" {
				cleanupErr = fmt.Errorf("verification schema survived cleanup")
			}
		}
		v.CleanupVerified = cleanupErr == nil
		err = errors.Join(err, cleanupErr)
	}()
	if _, err = v.cli("load-run", "load", "run", "--dataset", dataset, "--schema", v.Schema, "--env-file", envFile, "--target", "direct", "--output", filepath.Join(evidence, "load-run"), "--timeout", "2m"); err != nil {
		return err
	}
	audit := fmt.Sprintf(`SELECT (SELECT count(*) FROM pg_constraint WHERE connamespace='%[1]s'::regnamespace AND contype='p'), (SELECT count(*) FROM pg_constraint WHERE connamespace='%[1]s'::regnamespace AND contype='f' AND convalidated), (SELECT count(*) FROM %[1]s.fact_order_items JOIN %[1]s.fact_orders_header USING(order_id) JOIN %[1]s.dim_customers USING(customer_id) JOIN %[1]s.dim_products USING(product_id));`, v.Schema)
	observed, err := v.query(ctx, client, "import-audit", audit)
	if err != nil {
		return err
	}
	if observed != "7|8|39" {
		return fmt.Errorf("import audit = %q, want 7|8|39", observed)
	}
	lookup := fmt.Sprintf("version: 1\nname: verification-lookup\ndataset: dataset\nschema: %s\ntargets: [%s]\nworkload: customer-lookup\nseed: 0\nprotocol: simple\nconcurrency: [1]\nrepetitions: 2\ntarget_order: rotate\nwarmup: 0s\nduration: 1s\nthresholds:\n  max_p95_ms: 1000000000\n  max_error_rate: 0\n  min_tps: 0\n", v.Schema, targets)
	if err = v.write("lookup.yaml", lookup); err != nil {
		return err
	}
	for _, name := range []string{"baseline", "candidate"} {
		if _, err = v.bench(name, "lookup.yaml", 2*len(v.Targets)); err != nil {
			return err
		}
	}
	if _, err = v.cli("compare", "bench", "compare", filepath.Join(evidence, "baseline", "result.json"), filepath.Join(evidence, "candidate", "result.json"), "--output", filepath.Join(evidence, "comparison.json")); err != nil {
		return err
	}
	var comparison benchmark.Comparison
	if err = readJSON(filepath.Join(evidence, "comparison.json"), &comparison); err != nil {
		return err
	}
	if comparison.DatasetID != manifest.DatasetID || len(comparison.Steps) != len(v.Targets) {
		return fmt.Errorf("saved comparison is incomplete")
	}
	if err = v.write("read.sql", "SELECT 1;\n"); err != nil {
		return err
	}
	if v.requireMetrics {
		metricsScenario := "version: 1\nname: verification-periodic-metrics\ntargets: [direct]\nworkload: custom-sql\nsql_file: read.sql\nseed: 0\nprotocol: simple\nclients: 1\narrival_rates: [1]\nlatency_limit: 5s\nwarmup: 0s\nduration: 65s\nthresholds:\n  max_p95_ms: 1000000000\n  max_error_rate: 0\n  min_tps: 0\n  max_skipped_rate: 1\n  min_rate_attainment: 0.01\n"
		if err = v.write("metrics.yaml", metricsScenario); err != nil {
			return err
		}
		metricsResult, metricsErr := v.bench("metrics", "metrics.yaml", 1)
		if metricsErr != nil {
			return metricsErr
		}
		if len(metricsResult.SupabaseMetrics.Samples) < 2 {
			return fmt.Errorf("periodic metrics collection did not run")
		}
		periodic := metricsResult.SupabaseMetrics.Samples[1]
		measurement := metricsResult.Steps[0].Measurement
		if periodic.StartedAt.Before(measurement.StartedAt) || periodic.FinishedAt.After(measurement.FinishedAt) {
			return fmt.Errorf("periodic metrics snapshot did not overlap measured traffic")
		}
	}
	burst := fmt.Sprintf("version: 1\nname: verification-burst\ntargets: [%s]\nworkload: custom-sql\nsql_file: read.sql\nseed: 0\nprotocol: prepared\nconnection_mode: burst\nconcurrency: [1]\nwarmup: 0s\nduration: 10s\nthresholds:\n  max_p95_ms: 1000000000\n  max_error_rate: 0\n  min_tps: 0\n", targets)
	if err = v.write("burst.yaml", burst); err != nil {
		return err
	}
	burstResult, err := v.bench("burst", "burst.yaml", len(v.Targets))
	if err != nil {
		return err
	}
	for _, step := range burstResult.Steps {
		if step.Measurement.Metrics.CompletedIterations != 1 {
			return fmt.Errorf("burst did not execute exactly once per client")
		}
	}
	if err = v.write("setup.sql", fmt.Sprintf("CREATE TABLE IF NOT EXISTS %[1]s.verify_counter(id integer PRIMARY KEY, n bigint NOT NULL); INSERT INTO %[1]s.verify_counter VALUES(1,7) ON CONFLICT DO NOTHING; CREATE TABLE IF NOT EXISTS %[1]s.verify_audit(id bigint GENERATED ALWAYS AS IDENTITY, old_n bigint);", v.Schema)); err != nil {
		return err
	}
	if err = v.write("reset.sql", fmt.Sprintf("INSERT INTO %[1]s.verify_audit(old_n) SELECT n FROM %[1]s.verify_counter; UPDATE %[1]s.verify_counter SET n=0;", v.Schema)); err != nil {
		return err
	}
	if err = v.write("write.sql", "UPDATE "+v.Schema+".verify_counter SET n=n+1 WHERE id=1;\n"); err != nil {
		return err
	}
	writeScenario := fmt.Sprintf("version: 1\nname: verification-write-arrival\ntargets: [%s]\nworkload: custom-sql\nsql_file: write.sql\naccess_mode: read-write\nstate_policy: reset-per-phase\nsetup_file: setup.sql\nreset_file: reset.sql\nseed: 0\nprotocol: extended\nclients: 1\narrival_rates: [5]\nlatency_limit: 5s\nwarmup: 1s\nduration: 2s\nthresholds:\n  max_p95_ms: 1000000000\n  max_error_rate: 0\n  min_tps: 0\n  max_skipped_rate: 1\n  min_rate_attainment: 0.01\n", targets)
	if err = v.write("write.yaml", writeScenario); err != nil {
		return err
	}
	writeResult, err := v.bench("write", "write.yaml", len(v.Targets))
	if err != nil {
		return err
	}
	rows, err := v.query(ctx, client, "write-audit", "SELECT old_n FROM "+v.Schema+".verify_audit ORDER BY id;")
	if err != nil {
		return err
	}
	values := strings.Fields(rows)
	if len(values) != 3*len(writeResult.Steps) {
		return fmt.Errorf("missing lifecycle audit rows")
	}
	for i, step := range writeResult.Steps {
		before, _ := strconv.ParseInt(values[3*i], 10, 64)
		warmup, _ := strconv.ParseInt(values[3*i+1], 10, 64)
		measured, _ := strconv.ParseInt(values[3*i+2], 10, 64)
		initial := int64(0)
		if i == 0 {
			initial = 7
		}
		if before != initial || warmup < 1 || measured != step.Measurement.Metrics.CompletedIterations {
			return fmt.Errorf("write audit disagrees with completed work")
		}
	}
	observed, err = v.query(ctx, client, "write-restored", "SELECT n FROM "+v.Schema+".verify_counter;")
	if err != nil {
		return err
	}
	if observed != "0" {
		return fmt.Errorf("write workload did not restore its baseline")
	}
	if v.profiles {
		if err = v.profileExperiments(); err != nil {
			return err
		}
		v.ProfilesVerified = true
	}
	if v.requireMetrics {
		if err = v.verifyKeyAbsent(); err != nil {
			return err
		}
		v.MetricsVerified = true
	}
	v.Database = "verified"
	return nil
}

func (v *verification) profileExperiments() error {
	for _, profile := range []struct {
		name                string
		levels, repetitions int
	}{{"quick", 3, 1}, {"thorough", 4, 3}} {
		scenario := fmt.Sprintf("version: 1\nname: profile-%s\nprofile: %s\ndataset: dataset\nschema: %s\ntargets: [%s]\nworkload: customer-lookup\nseed: 0\nprotocol: simple\nthresholds:\n  max_p95_ms: 250\n  max_error_rate: 0\n  min_tps: 1\n", profile.name, profile.name, v.Schema, strings.Join(v.Targets, ","))
		name := "profile-" + profile.name
		if err := v.write(name+".yaml", scenario); err != nil {
			return err
		}
		_, runErr := v.cli(name, "bench", "run", "--scenario", filepath.Join(v.Evidence, name+".yaml"), "--env-file", v.envFile, "--output", filepath.Join(v.Evidence, name))
		result, err := benchmark.ReadResult(filepath.Join(v.Evidence, name, "result.json"))
		if err != nil {
			return errors.Join(runErr, err)
		}
		if len(result.Steps) != len(v.Targets)*profile.levels*profile.repetitions || result.Scenario.Profile != profile.name || result.Scenario.ProfileVersion != 1 {
			return fmt.Errorf("%s profile coverage is incomplete", name)
		}
		for _, step := range result.Steps {
			if step.Measurement == nil || step.Measurement.Status != "completed" || (step.Status != "passed" && step.Status != "failed") {
				return fmt.Errorf("%s has execution failures", name)
			}
		}
		if runErr != nil && result.Status != "failed" {
			return runErr
		}
		if err := v.checkMetrics(name, result); err != nil {
			return err
		}
	}
	return nil
}

func (v *verification) verifyKeyAbsent() error {
	settings := map[string]string{}
	if v.envFile != "" {
		var err error
		settings, err = godotenv.Read(v.envFile)
		if err != nil {
			return fmt.Errorf("cannot read environment for credential audit")
		}
	}
	key := settings["SUPABASE_API_KEY"]
	if value, present := os.LookupEnv("SUPABASE_API_KEY"); present {
		key = value
	}
	if key == "" {
		return fmt.Errorf("credential audit requires SUPABASE_API_KEY")
	}
	return filepath.WalkDir(v.Evidence, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		switch filepath.Ext(path) {
		case ".json", ".log", ".prom":
			contents, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			if bytes.Contains(contents, []byte(key)) || bytes.Contains(contents, []byte(base64.StdEncoding.EncodeToString([]byte("service_role:"+key)))) {
				return fmt.Errorf("Supabase API credential found in verification evidence")
			}
		}
		return nil
	})
}

func (v *verification) command(name string, env []string, executable string, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(v.ctx, executable, args...)
	cmd.Env = append(os.Environ(), env...)
	output, err := cmd.CombinedOutput()
	record := struct {
		Command  []string `json:"command"`
		ExitCode int      `json:"exit_code"`
	}{append([]string{executable}, args...), 0}
	if err != nil {
		record.ExitCode = -1
		var exit *exec.ExitError
		if errors.As(err, &exit) {
			record.ExitCode = exit.ExitCode()
		}
	}
	logErr := errors.Join(v.write(name+".log", string(output)), v.save(name+".command.json", record))
	if err != nil {
		return output, errors.Join(fmt.Errorf("%s failed; inspect %s: %w", name, filepath.Join(v.Evidence, name+".log"), err), logErr)
	}
	fmt.Println("PASS", name)
	return output, logErr
}

func (v *verification) cli(name string, args ...string) ([]byte, error) {
	return v.command(name, nil, v.binary, args...)
}
func (v *verification) write(name, contents string) error {
	return os.WriteFile(filepath.Join(v.Evidence, name), []byte(contents), 0600)
}
func (v *verification) save(name string, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	return v.write(name, string(data)+"\n")
}
func readJSON(path string, value any) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, value)
}

func (v *verification) query(ctx context.Context, client *postgres.Client, name, sql string) (string, error) {
	var output, diagnostics bytes.Buffer
	err := client.Run(ctx, []string{"-At", "--command", sql}, &output, &diagnostics)
	err = errors.Join(err, v.write(name+".sql", sql+"\n"), v.write(name+".log", output.String()+diagnostics.String()))
	return strings.TrimSpace(output.String()), err
}

func (v *verification) bench(name, scenario string, expected int) (*benchmark.Result, error) {
	if _, err := v.cli(name, "bench", "run", "--scenario", filepath.Join(v.Evidence, scenario), "--env-file", v.envFile, "--output", filepath.Join(v.Evidence, name)); err != nil {
		return nil, err
	}
	result, err := benchmark.ReadResult(filepath.Join(v.Evidence, name, "result.json"))
	if err != nil {
		return nil, err
	}
	if result.Status != "completed" || len(result.Steps) != expected {
		return nil, fmt.Errorf("%s has missing/failed steps", name)
	}
	if err := v.checkMetrics(name, result); err != nil {
		return nil, err
	}
	for _, step := range result.Steps {
		if step.Status != "passed" || step.Measurement == nil || step.Measurement.Metrics == nil || step.Measurement.Metrics.CompletedIterations < 1 || step.Measurement.Metrics.FailedIterations != 0 {
			return nil, fmt.Errorf("%s has failed or empty measurements", name)
		}
	}
	return result, nil
}

func (v *verification) checkMetrics(name string, result *benchmark.Result) error {
	if v.requireMetrics {
		telemetry := result.SupabaseMetrics
		if telemetry == nil || telemetry.Status != "available" || len(telemetry.Samples) == 0 || telemetry.IntervalSeconds != 60 || telemetry.Scope != "project" {
			return fmt.Errorf("%s did not collect live Supabase metrics; inspect result.json", name)
		}
		for _, sample := range telemetry.Samples {
			if sample.Status != "collected" || sample.Series < 1 || sample.StartedAt.IsZero() || sample.FinishedAt.Before(sample.StartedAt) || filepath.Base(sample.File) != sample.File {
				return fmt.Errorf("%s has an incomplete metrics snapshot", name)
			}
			contents, err := os.ReadFile(filepath.Join(v.Evidence, name, sample.File))
			if err != nil {
				return err
			}
			hash := sha256.Sum256(contents)
			if hex.EncodeToString(hash[:]) != sample.SHA256 {
				return fmt.Errorf("%s metrics snapshot checksum differs", name)
			}
		}
	}
	return nil
}

func verifyCSV(dir string, manifest core.Manifest) error {
	counts := map[string]int64{}
	for _, entry := range manifest.Files {
		file, err := os.Open(filepath.Join(dir, entry.Name))
		if err != nil {
			return err
		}
		reader := csv.NewReader(file)
		_, err = reader.Read()
		var count int64
		for err == nil {
			_, err = reader.Read()
			if err == nil {
				count++
			}
		}
		closeErr := file.Close()
		if err != io.EOF || closeErr != nil || count != entry.Rows {
			return fmt.Errorf("CSV row count mismatch for %s", entry.Name)
		}
		counts[entry.Table] += count
	}
	for table, want := range manifest.Identity.Counts {
		if counts[table] != want {
			return fmt.Errorf("CSV total mismatch for %s", table)
		}
	}
	return nil
}
