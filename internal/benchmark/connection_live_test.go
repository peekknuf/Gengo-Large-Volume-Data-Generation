package benchmark

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/peekknuf/Gengo/internal/postgres"
)

func TestConnectionMetricsLive(t *testing.T) {
	envFile := os.Getenv("GENGO_POSTGRES_TEST_ENV_FILE")
	if envFile == "" {
		t.Skip("set GENGO_POSTGRES_TEST_ENV_FILE to opt into connection experiments")
	}
	client, err := postgres.NewClient("direct", envFile)
	if err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"reconnect", "burst"} {
		t.Run(mode, func(t *testing.T) {
			root := t.TempDir()
			script := filepath.Join(root, "query.sql")
			if err := os.WriteFile(script, []byte("SELECT 1;\n"), 0600); err != nil {
				t.Fatal(err)
			}
			args := []string{"--no-vacuum", "--connect", "--protocol=simple", "--max-tries=1", "--client=2", "--file=" + script, "--log", "--log-prefix=" + filepath.Join(root, "transactions")}
			if mode == "burst" {
				args = append(args, "--jobs=2", "--transactions=1")
			} else {
				args = append(args, "--time=2")
			}
			var output, diagnostics bytes.Buffer
			if err := client.RunPgbench(t.Context(), args, &output, &diagnostics); err != nil {
				t.Fatalf("pgbench: %v\n%s", err, diagnostics.String())
			}
			metrics, err := readMetrics(output.String(), filepath.Join(root, "transactions"), 0, mode)
			if err != nil {
				t.Fatal(err)
			}
			if metrics.TPSIncludingConnections == nil || *metrics.TPSIncludingConnections <= 0 || metrics.ConnectionMeanMS == nil || *metrics.ConnectionMeanMS <= 0 || metrics.LatencyBasis != "transaction-start-to-completion" {
				t.Fatalf("missing reconnect measurements: %+v", metrics)
			}
			if mode == "burst" && (metrics.CompletedIterations != 2 || metrics.FailedIterations != 0) {
				t.Fatalf("burst was not one iteration per client: %+v", metrics)
			}
			if !strings.Contains(output.String(), "including reconnection times") {
				t.Fatal("throughput did not include connection setup")
			}
		})
	}
}

func TestConnectionIdentityLive(t *testing.T) {
	envFile := os.Getenv("GENGO_POSTGRES_TEST_ENV_FILE")
	if envFile == "" {
		t.Skip("set GENGO_POSTGRES_TEST_ENV_FILE to opt into direct connection identity checks")
	}
	client, err := postgres.NewClient("direct", envFile)
	if err != nil {
		t.Fatal(err)
	}
	namespace := fmt.Sprintf("gengo_connections_%d", time.Now().UnixNano())
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		var output bytes.Buffer
		if err := client.Run(ctx, []string{"--command", "DROP SCHEMA IF EXISTS " + namespace + " CASCADE;"}, &output, &output); err != nil {
			t.Errorf("cleanup: %v\n%s", err, output.String())
		}
	})
	query := func(sql string) string {
		t.Helper()
		var output, diagnostics bytes.Buffer
		if err := client.Run(t.Context(), []string{"-At", "--command", sql}, &output, &diagnostics); err != nil {
			t.Fatalf("inspect connections: %v\n%s", err, diagnostics.String())
		}
		return strings.TrimSpace(output.String())
	}
	query("CREATE SCHEMA " + namespace + "; CREATE TABLE " + namespace + ".audit (pid int);")
	for _, mode := range []string{"persistent", "reconnect", "burst", "arrival-reconnect"} {
		t.Run(mode, func(t *testing.T) {
			query("TRUNCATE " + namespace + ".audit;")
			root := t.TempDir()
			connection, loads := mode, "concurrency: [2]"
			if mode == "arrival-reconnect" {
				connection = "reconnect"
				loads = "clients: 2\narrival_rates: [2]\nlatency_limit: 1s"
			}
			scenario := fmt.Sprintf("version: 1\nname: connection-identity\nworkload: custom-sql\nsql_file: query.sql\naccess_mode: read-write\nstate_policy: reuse\ntargets: [direct]\nseed: 0\nprotocol: simple\nconnection_mode: %s\n%s\nwarmup: 0s\nduration: 5s\nthresholds:\n  max_p95_ms: 1000000000\n  max_error_rate: 0\n  min_tps: 0\n", connection, loads)
			if mode == "arrival-reconnect" {
				scenario += "  min_rate_attainment: 0.01\n  max_skipped_rate: 1\n"
			}
			for name, contents := range map[string]string{"scenario.yaml": scenario, "query.sql": "INSERT INTO " + namespace + ".audit SELECT pg_backend_pid();"} {
				if err := os.WriteFile(filepath.Join(root, name), []byte(contents), 0600); err != nil {
					t.Fatal(err)
				}
			}
			plan, err := LoadScenario(filepath.Join(root, "scenario.yaml"))
			if err != nil {
				t.Fatal(err)
			}
			result, err := Run(t.Context(), plan, envFile, filepath.Join(root, "result"), &bytes.Buffer{})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := ReadResult(filepath.Join(root, "result", "result.json")); err != nil {
				t.Fatal(err)
			}
			metrics := result.Steps[0].Measurement.Metrics
			var rows, connections int64
			if _, err := fmt.Sscanf(query("SELECT count(*), count(DISTINCT pid) FROM "+namespace+".audit;"), "%d|%d", &rows, &connections); err != nil {
				t.Fatal(err)
			}
			if rows != metrics.CompletedIterations {
				t.Fatalf("committed writes differ from measured iterations: %d rows, %d iterations", rows, metrics.CompletedIterations)
			}
			if mode == "persistent" || mode == "burst" {
				if connections != 2 {
					t.Fatalf("expected two persistent/burst sessions, got %d", connections)
				}
			} else if connections != rows || connections <= 2 {
				t.Fatalf("reconnect reused database sessions: %d sessions for %d iterations", connections, rows)
			}
		})
	}
}
