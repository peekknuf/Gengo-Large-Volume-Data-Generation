package tests

import (
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/peekknuf/Gengo/internal/postgres"
)

func TestGenerationControls(t *testing.T) {
	name := "gengo"
	if runtime.GOOS == "windows" {
		name += ".exe"
	}
	bin := filepath.Join(t.TempDir(), name)
	build := exec.CommandContext(t.Context(), "go", "build", "-o", bin, ".")
	build.Dir = moduleRoot(t)
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}
	run := func(t *testing.T, args ...string) ([]byte, error) {
		t.Helper()
		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()
		cmd := exec.CommandContext(ctx, bin, args...)
		cmd.Dir = t.TempDir()
		return cmd.CombinedOutput()
	}
	t.Run("load_plan", func(t *testing.T) {
		root := t.TempDir()
		dataset := filepath.Join(root, "dataset")
		out, err := run(t, "gen", "--model", "ecommerce", "--size", "0.000001", "--format", "csv", "--seed", "0", "--as-of", "2026-11-21", "--output", dataset)
		if err != nil {
			t.Fatalf("generate fixture: %v\n%s", err, out)
		}
		output := filepath.Join(root, "import")
		out, err = run(t, "load", "plan", "--dataset", dataset, "--schema", "gengo_cli", "--output", output)
		if err != nil {
			t.Fatalf("load plan: %v\n%s", err, out)
		}
		ddl, err := os.ReadFile(filepath.Join(output, "schema.sql"))
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(ddl), `CREATE SCHEMA "gengo_cli";`) {
			t.Fatal("load command did not use the requested schema")
		}
		for _, name := range []string{"run.psql", "load.sql", "indexes.sql", "foreign_keys.sql", "validate_foreign_keys.sql", "verify.sql", "analyze.sql"} {
			if _, err := os.Stat(filepath.Join(output, name)); err != nil {
				t.Fatal(err)
			}
		}
		if out, err := run(t, "load", "plan", "--dataset", dataset, "--output", output); err == nil {
			t.Fatalf("overwrote existing scripts: %s", out)
		}
		if out, err := run(t, "load", "plan", "--dataset", dataset); err == nil {
			t.Fatalf("accepted missing output flag: %s", out)
		}
	})
	t.Run("database_validation", func(t *testing.T) {
		for _, arguments := range [][]string{
			{"db", "check", "--target", "unknown"},
			{"db", "check", "--timeout", "0s"},
			{"db", "check", "--env-file", filepath.Join(t.TempDir(), "missing")},
			{"load", "run", "--dataset", "missing", "--output", filepath.Join(t.TempDir(), "import"), "--target", "shared"},
			{"load", "run", "--dataset", "missing", "--output", filepath.Join(t.TempDir(), "import"), "--target", "dedicated"},
			{"load", "run", "--dataset", "missing", "--output", filepath.Join(t.TempDir(), "import"), "--timeout", "-1s"},
			{"load", "run", "--dataset", "missing"},
		} {
			if output, err := run(t, arguments...); err == nil {
				t.Fatalf("accepted invalid database arguments %v: %s", arguments, output)
			}
		}
	})
	t.Run("load_run_live", func(t *testing.T) {
		envFile := os.Getenv("GENGO_POSTGRES_TEST_ENV_FILE")
		if envFile == "" {
			t.Skip("set GENGO_POSTGRES_TEST_ENV_FILE to opt into live CLI imports")
		}
		client, err := postgres.NewClient("direct", envFile)
		if err != nil {
			t.Fatal(err)
		}
		namespace := fmt.Sprintf("gengo_cli_%d", time.Now().UnixNano())
		t.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			var output bytes.Buffer
			if err := client.Run(ctx, []string{"--command", `DROP SCHEMA IF EXISTS "` + namespace + `" CASCADE;`}, &output, &output); err != nil {
				t.Errorf("cleanup %s: %v\n%s", namespace, err, output.String())
			}
		})
		root := t.TempDir()
		dataset := filepath.Join(root, "dataset")
		if output, err := run(t, "gen", "--model", "ecommerce", "--size", "0.000001", "--format", "csv", "--seed", "0", "--as-of", "2026-11-21", "--output", dataset); err != nil {
			t.Fatalf("generate: %v\n%s", err, output)
		}
		if output, err := run(t, "db", "check", "--env-file", envFile); err != nil || !strings.Contains(string(output), "PostgreSQL connection verified (direct)") {
			t.Fatalf("connection check: %v\n%s", err, output)
		}
		arguments := []string{"load", "run", "--dataset", dataset, "--schema", namespace, "--output", filepath.Join(root, "import"), "--env-file", envFile}
		if output, err := run(t, arguments...); err != nil || !strings.Contains(string(output), "PostgreSQL import verified in schema "+namespace) {
			t.Fatalf("live CLI import: %v\n%s", err, output)
		}
		var output bytes.Buffer
		query := fmt.Sprintf("SELECT count(*) FROM pg_constraint WHERE connamespace = '%s'::regnamespace AND contype IN ('p', 'f') AND convalidated;", namespace)
		if err := client.Run(t.Context(), []string{"-At", "--command", query}, &output, &output); err != nil || strings.TrimSpace(output.String()) != "15" {
			t.Fatalf("expected 15 validated constraints: %v\n%s", err, output.String())
		}
		if output, err := run(t, "load", "run", "--dataset", dataset, "--schema", namespace, "--output", filepath.Join(root, "retry-import"), "--env-file", envFile); err == nil || !strings.Contains(string(output), "already exists") {
			t.Fatalf("existing schema not rejected: %v\n%s", err, output)
		}
	})
	t.Run("exact_rows", func(t *testing.T) {
		dir := filepath.Join(t.TempDir(), "financial")
		out, err := run(t, "gen", "--model", "financial", "--format", "csv", "--size", "0.001", "--rows", "companies=3", "--rows", "daily_stock_prices=7", "--output", dir)
		if err != nil {
			t.Fatalf("%v: %s", err, out)
		}

		files, err := filepath.Glob(filepath.Join(dir, "fact_daily_stock_prices_*.csv"))
		if err != nil {
			t.Fatal(err)
		}
		count := 0
		for _, path := range files {
			f, err := os.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			rows, err := csv.NewReader(f).ReadAll()
			f.Close()
			if err != nil {
				t.Fatal(err)
			}
			count += len(rows) - 1
		}
		if count != 7 {
			t.Fatalf("generated %d prices, want 7", count)
		}

	})
	t.Run("reject_row_overrides", func(t *testing.T) {
		for _, arg := range []string{"unknown=1", "patients=-1", "patients=0", "doctors=999999999999999999999"} {
			if out, err := run(t, "gen", "--model", "medical", "--format", "csv", "--size", "0.001", "--rows", arg, "--dry-run"); err == nil {
				t.Fatalf("accepted %s: %s", arg, out)
			}
		}
	})
	t.Run("dry_run", func(t *testing.T) {
		output := filepath.Join(t.TempDir(), "not-created")
		out, err := run(t, "gen", "--model", "medical", "--size", "0.001", "--format", "csv", "--workers", "2", "--dry-run", "--output", output)
		if err != nil {
			t.Fatalf("dry run: %v\n%s", err, out)
		}
		if !strings.Contains(string(out), "Dry run complete") {
			t.Fatalf("missing dry-run result: %s", out)
		}
		if _, err := os.Stat(output); !os.IsNotExist(err) {
			t.Fatalf("unexpected output: %v", err)
		}
	})
	t.Run("dry_run_without_output", func(t *testing.T) {
		out, err := run(t, "gen", "--model", "medical", "--size", "0.001", "--format", "csv", "--workers", "2", "--dry-run")
		if err != nil {
			t.Fatalf("dry run without output: %v\n%s", err, out)
		}
	})
	t.Run("invalid_workers", func(t *testing.T) {
		for _, count := range []string{"0", "-1"} {
			out, err := run(t, "gen", "--workers", count, "--dry-run")
			if err == nil || !strings.Contains(string(out), "workers must be at least 1") {
				t.Fatalf("workers %s: err=%v out=%s", count, err, out)
			}
		}
	})
	t.Run("stable_shards", func(t *testing.T) {
		output := t.TempDir()
		out, err := run(t, "gen", "--model", "ecommerce-ds", "--size", "0.0001", "--format", "csv", "--workers", "2", "--rows-per-file", "4", "--rows", "store_sales=7", "--rows", "catalog_sales=7", "--rows", "web_sales=7", "--seed", "0", "--as-of", "2026-11-21", "--output", output)
		if err != nil {
			t.Fatalf("generation: %v\n%s", err, out)
		}
		for _, table := range []string{"fact_store_sales", "fact_catalog_sales", "fact_web_sales"} {
			files, err := filepath.Glob(filepath.Join(output, table+"_*.csv"))
			if err != nil || len(files) != 2 {
				t.Errorf("%s: shards=%v err=%v", table, files, err)
			}
		}
	})
}
