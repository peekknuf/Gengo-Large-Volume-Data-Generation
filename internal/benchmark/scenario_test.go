package benchmark

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/peekknuf/Gengo/internal/core"
)

const validScenario = `version: 1
name: lookup-test
dataset: dataset
schema: gengo_test
targets: [direct, shared, dedicated]
workload: customer-lookup
seed: 0
protocol: simple
concurrency: [1, 2]
warmup: 1s
duration: 2s
thresholds:
  max_p95_ms: 100
  max_error_rate: 0.01
  min_tps: 10
`

func scenarioFixture(t *testing.T, contents string) string {
	t.Helper()
	directory := t.TempDir()
	dataset := filepath.Join(directory, "dataset")
	if err := os.Mkdir(dataset, 0755); err != nil {
		t.Fatal(err)
	}
	manifest := core.Manifest{Version: 1, Complete: true, Identity: core.DatasetIdentity{SchemaVersion: core.SchemaVersion, Model: "ecommerce", Counts: map[string]int64{"dim_customers": 3}}}
	identity, err := json.Marshal(manifest.Identity)
	if err != nil {
		t.Fatal(err)
	}
	hash := sha256.Sum256(identity)
	manifest.DatasetID = hex.EncodeToString(hash[:])
	encoded, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dataset, "manifest.json"), encoded, 0600); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(directory, "scenario.yaml")
	if err := os.WriteFile(path, []byte(contents), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLoadScenario(t *testing.T) {
	path := scenarioFixture(t, validScenario)
	plan, err := LoadScenario(path)
	if err != nil {
		t.Fatal(err)
	}
	if *plan.Scenario.Seed != 0 || plan.Warmup != time.Second || plan.Duration != 2*time.Second || plan.Dataset.Identity.Counts["dim_customers"] != 3 || plan.Scenario.Dataset != filepath.Join(filepath.Dir(path), "dataset") {
		t.Fatalf("unexpected resolved plan: %+v", plan)
	}
	if len(plan.Scenario.Targets) != 3 || plan.Scenario.Targets[2] != "dedicated" || plan.Scenario.Concurrency[1] != 2 {
		t.Fatalf("incorrect endpoint/load plan: %+v", plan.Scenario)
	}
}

func TestLoadScenarioRejectsInvalidInputs(t *testing.T) {
	for _, testCase := range []struct{ name, old, replacement string }{
		{"version", "version: 1", "version: 2"},
		{"unknown_field", "name: lookup-test", "name: lookup-test\npassword: never-echo-me"},
		{"documents", "version: 1", "version: 1\n---\nversion: 1"},
		{"missing_seed", "seed: 0\n", ""},
		{"negative_seed", "seed: 0", "seed: -1"},
		{"large_seed", "seed: 0", "seed: 18446744073709551615"},
		{"schema", "schema: gengo_test", "schema: public;DROP TABLE x"},
		{"workload", "workload: customer-lookup", "workload: write"},
		{"protocol", "protocol: simple", "protocol: prepared"},
		{"target", "targets: [direct, shared, dedicated]", "targets: [unknown]"},
		{"duplicate_target", "targets: [direct, shared, dedicated]", "targets: [direct, direct]"},
		{"no_target", "targets: [direct, shared, dedicated]", "targets: []"},
		{"zero_clients", "concurrency: [1, 2]", "concurrency: [0]"},
		{"unordered_clients", "concurrency: [1, 2]", "concurrency: [2, 1]"},
		{"duplicate_clients", "concurrency: [1, 2]", "concurrency: [1, 1]"},
		{"too_many_clients", "concurrency: [1, 2]", "concurrency: [1025]"},
		{"fractional_duration", "duration: 2s", "duration: 1500ms"},
		{"unbounded_duration", "duration: 2s", "duration: 6m"},
		{"zero_duration", "duration: 2s", "duration: 0s"},
		{"negative_warmup", "warmup: 1s", "warmup: -1s"},
		{"missing_warmup", "warmup: 1s\n", ""},
		{"negative_latency", "max_p95_ms: 100", "max_p95_ms: -1"},
		{"nan_latency", "max_p95_ms: 100", "max_p95_ms: .nan"},
		{"infinite_rate", "min_tps: 10", "min_tps: .inf"},
		{"bad_error_rate", "max_error_rate: 0.01", "max_error_rate: 2"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			path := scenarioFixture(t, strings.Replace(validScenario, testCase.old, testCase.replacement, 1))
			if _, err := LoadScenario(path); err == nil || strings.Contains(err.Error(), "never-echo-me") {
				t.Fatalf("expected credential-safe rejection, got %v", err)
			}
		})
	}
}

func TestLoadScenarioRejectsChangedManifest(t *testing.T) {
	path := scenarioFixture(t, validScenario)
	manifest := filepath.Join(filepath.Dir(path), "dataset", "manifest.json")
	contents, err := os.ReadFile(manifest)
	if err != nil {
		t.Fatal(err)
	}
	contents = []byte(strings.Replace(string(contents), `"dim_customers":3`, `"dim_customers":4`, 1))
	if err := os.WriteFile(manifest, contents, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadScenario(path); err == nil {
		t.Fatal("accepted manifest with inconsistent dataset identity")
	}
}
