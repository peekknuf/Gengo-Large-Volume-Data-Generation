package tests

import (
	"context"
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
	t.Run("worker_shards", func(t *testing.T) {
		output := t.TempDir()
		out, err := run(t, "gen", "--model", "ecommerce-ds", "--size", "0.0001", "--format", "csv", "--workers", "2", "--output", output)
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
