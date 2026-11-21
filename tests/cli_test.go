package tests

import (
	"context"
	"encoding/csv"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
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
