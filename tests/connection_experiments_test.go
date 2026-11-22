package tests

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/peekknuf/Gengo/internal/benchmark"
)

func TestConnectionExperimentsLive(t *testing.T) {
	envFile := os.Getenv("GENGO_POSTGRES_TEST_ENV_FILE")
	if envFile == "" {
		t.Skip("set GENGO_POSTGRES_TEST_ENV_FILE to opt into connection experiments")
	}
	targets := os.Getenv("GENGO_POSTGRES_TEST_TARGETS")
	if targets == "" {
		targets = "direct"
	}
	bin := binaryPath(t)
	build := exec.CommandContext(t.Context(), "go", "build", "-o", bin, ".")
	build.Dir = moduleRoot(t)
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, output)
	}
	for _, mode := range []string{"persistent", "reconnect", "burst"} {
		for _, protocol := range []string{"simple", "extended", "prepared"} {
			t.Run(mode+"_"+protocol, func(t *testing.T) {
				root := t.TempDir()
				scenario := fmt.Sprintf("version: 1\nname: connections\nworkload: custom-sql\nsql_file: query.sql\ntargets: [%s]\nseed: 0\nprotocol: %s\nconnection_mode: %s\nconcurrency: [1, 2]\nwarmup: 0s\nduration: 3s\nthresholds:\n  max_p95_ms: 1000000000\n  max_error_rate: 0\n  min_tps: 0\n", targets, protocol, mode)
				if mode == "burst" {
					scenario = strings.ReplaceAll(scenario, "duration: 3s", "duration: 10s")
				}
				for name, contents := range map[string]string{"scenario.yaml": scenario, "query.sql": "\\set id random(1, 100)\nSELECT :id;"} {
					if err := os.WriteFile(filepath.Join(root, name), []byte(contents), 0600); err != nil {
						t.Fatal(err)
					}
				}
				cmd := exec.CommandContext(t.Context(), bin, "bench", "run", "--scenario", filepath.Join(root, "scenario.yaml"), "--env-file", envFile, "--output", filepath.Join(root, "result"))
				cmd.Dir = t.TempDir()
				output, err := cmd.CombinedOutput()
				if err != nil {
					t.Fatalf("connection CLI: %v\n%s", err, output)
				}
				result, err := benchmark.ReadResult(filepath.Join(root, "result", "result.json"))
				if err != nil {
					t.Fatal(err)
				}
				if len(result.Steps) != 2*len(strings.Split(targets, ",")) || result.Scenario.ConnectionMode != mode || result.Scenario.Protocol != protocol {
					t.Fatalf("missing mode/protocol metadata: %+v", result)
				}
				for _, step := range result.Steps {
					log, err := os.ReadFile(filepath.Join(root, "result", fmt.Sprintf("%s-c%d-measurement", step.Target, step.Concurrency), "pgbench.txt"))
					if err != nil || !bytes.Contains(log, []byte("query mode: "+protocol)) {
						t.Fatalf("pgbench did not confirm requested protocol %s: %v", protocol, err)
					}
					metrics := step.Measurement.Metrics
					if step.Status != "passed" || metrics.CompletedIterations < 1 || result.Capacity[step.Target] != 2 {
						t.Fatalf("incomplete connection sweep: %+v", step)
					}
					if mode != "persistent" && (metrics.TPSIncludingConnections == nil || *metrics.TPSIncludingConnections <= 0 || metrics.ConnectionMeanMS == nil || *metrics.ConnectionMeanMS <= 0 || metrics.ThroughputBasis != "including-connection-times" || metrics.LatencyBasis != "transaction-start-to-completion" || metrics.TPS != 0) {
						t.Fatalf("missing connection measurements: %+v", metrics)
					}
					if mode == "burst" && (metrics.RequestedIterations == nil || *metrics.RequestedIterations != int64(step.Concurrency) || metrics.CompletedIterations != int64(step.Concurrency) || result.LoadModel != "connection-burst") {
						t.Fatalf("burst was not bounded to one iteration per client: %+v", step)
					}
				}
				compare := exec.CommandContext(t.Context(), bin, "bench", "compare", filepath.Join(root, "result", "result.json"), filepath.Join(root, "result", "result.json"))
				compare.Dir = t.TempDir()
				if output, err := compare.CombinedOutput(); err != nil || !bytes.Contains(output, []byte(mode+"/"+protocol)) {
					t.Fatalf("offline connection comparison: %v\n%s", err, output)
				}
			})
		}
	}
}
