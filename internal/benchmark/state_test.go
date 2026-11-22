package benchmark

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestStatePolicies(t *testing.T) {
	for _, test := range []struct {
		name, fields string
		valid        bool
	}{
		{"legacy", "", true},
		{"explicit_read", "access_mode: read-only\nstate_policy: read-only-reuse\n", true},
		{"write_needs_policy", "access_mode: read-write\n", false},
		{"write_reuse", "access_mode: read-write\nstate_policy: reuse\n", true},
		{"write_reset", "access_mode: read-write\nstate_policy: reset-per-phase\nreset_file: reset.sql\nsetup_file: setup.sql\n", true},
		{"read_reset", "state_policy: reset-per-phase\nreset_file: reset.sql\n", true},
		{"missing_reset", "state_policy: reset-per-phase\n", false},
		{"reuse_reset", "state_policy: reuse\nreset_file: reset.sql\n", false},
		{"read_only_setup", "setup_file: setup.sql\n", false},
		{"unknown_access", "access_mode: write\n", false},
		{"unknown_policy", "state_policy: reset\n", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := customScenario(t, "SELECT 1;", false)
			for _, name := range []string{"setup.sql", "reset.sql"} {
				if err := os.WriteFile(filepath.Join(filepath.Dir(path), name), []byte("SELECT 1;"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0600)
			if err != nil {
				t.Fatal(err)
			}
			_, err = file.WriteString(test.fields)
			file.Close()
			if err != nil {
				t.Fatal(err)
			}
			plan, err := LoadScenario(path)
			if (err == nil) != test.valid {
				t.Fatalf("valid=%t: %v", test.valid, err)
			}
			if test.valid && strings.Contains(test.fields, "access_mode: read-write") && plan.SQL != "BEGIN READ WRITE;\nSELECT 1;\nCOMMIT;\n" {
				t.Fatalf("write transaction not configured: %q", plan.SQL)
			}
			if test.valid && plan.Scenario.ResetFile != "" && (plan.ResetSQL != "SELECT 1;" || !filepath.IsAbs(plan.Scenario.ResetFile)) {
				t.Fatalf("reset not snapshotted: %+v", plan)
			}
		})
	}
	if _, err := LoadScenario(scenarioFixture(t, validScenario+"access_mode: read-write\nstate_policy: reuse\n")); err == nil {
		t.Fatal("built-in lookup accepted write access")
	}
}

func TestStateLifecycleExecution(t *testing.T) {
	installClientHelpers(t)
	t.Setenv("GENGO_BENCH_HELPER", "state")
	for _, failure := range []string{"", "setup", "reset-1", "reset-2", "reset-3", "measurement", "cancel"} {
		t.Run(failure, func(t *testing.T) {
			path := customScenario(t, "SELECT 1;", false)
			for _, name := range []string{"setup.sql", "reset.sql"} {
				if err := os.WriteFile(filepath.Join(filepath.Dir(path), name), []byte("SELECT 1;"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0600)
			if err != nil {
				t.Fatal(err)
			}
			_, err = file.WriteString("access_mode: read-write\nstate_policy: reset-per-phase\nsetup_file: setup.sql\nreset_file: reset.sql\n")
			file.Close()
			if err != nil {
				t.Fatal(err)
			}
			plan, err := LoadScenario(path)
			if err != nil {
				t.Fatal(err)
			}
			plan.Scenario.Targets = []string{"direct"}
			plan.Scenario.Thresholds.MaxErrorRate = 0.2
			for _, name := range []string{"setup.sql", "reset.sql"} {
				if err := os.WriteFile(filepath.Join(filepath.Dir(path), name), []byte("SELECT 1/0;"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			events := filepath.Join(t.TempDir(), "events.txt")
			t.Setenv("GENGO_STATE_EVENTS", events)
			t.Setenv("GENGO_STATE_FAIL", failure)
			output := filepath.Join(t.TempDir(), "result")
			ctx := t.Context()
			if failure == "cancel" {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, time.Second)
				defer cancel()
			}
			result, err := Run(ctx, plan, "", output, &bytes.Buffer{})
			if result == nil || (err == nil) != (failure == "") {
				t.Fatalf("unexpected lifecycle result: %+v, %v", result, err)
			}
			contents, readErr := os.ReadFile(events)
			if readErr != nil {
				t.Fatal(readErr)
			}
			want := "setup\nreset\nwarmup\nreset\nmeasurement\nreset\nreset\nwarmup\nreset\nmeasurement\nreset\n"
			switch failure {
			case "setup":
				want = "setup\n"
			case "reset-1":
				want = "setup\nreset\n"
			case "reset-2":
				want = "setup\nreset\nwarmup\nreset\n"
			case "reset-3":
				want = "setup\nreset\nwarmup\nreset\nmeasurement\nreset\n"
			case "cancel":
				want = "setup\nreset\nwarmup\n"
				if result.Status != "canceled" || result.Steps[0].ResetAfter != nil {
					t.Fatalf("cancellation lost state: %+v", result)
				}
			}
			if string(contents) != want {
				t.Fatalf("wrong lifecycle order:\ngot %q\nwant %q", contents, want)
			}
			saved, readErr := os.ReadFile(filepath.Join(output, "result.json"))
			if readErr != nil || bytes.Contains(saved, []byte("private-helper-password")) {
				t.Fatal("missing or unredacted lifecycle result")
			}
			if failure == "" {
				for _, name := range []string{"setup.sql", "reset.sql"} {
					contents, err := os.ReadFile(filepath.Join(output, name))
					if err != nil || string(contents) != "SELECT 1;" {
						t.Fatalf("lifecycle snapshot changed: %q, %v", contents, err)
					}
				}
				loaded, err := ReadResult(filepath.Join(output, "result.json"))
				if err != nil {
					t.Fatal(err)
				}
				loaded.Scenario.SetupFile = "/moved/setup.sql"
				loaded.Scenario.ResetFile = "/moved/reset.sql"
				if _, err := Compare(result, loaded); err != nil {
					t.Fatalf("moved lifecycle scripts rejected: %v", err)
				}
				loaded.ResetSHA256 = strings.Repeat("a", 64)
				if _, err := Compare(result, loaded); err == nil || !strings.Contains(err.Error(), "reset SQL identity") {
					t.Fatalf("changed reset script compared: %v", err)
				}
				loaded.ResetSHA256 = result.ResetSHA256
				loaded.Steps[0].ResetAfter = nil
				if _, err := Compare(result, loaded); err == nil {
					t.Fatal("missing final reset compared")
				}
			} else if _, err := ReadResult(filepath.Join(output, "result.json")); err == nil {
				t.Fatal("incomplete lifecycle accepted for comparison")
			}
		})
	}
}
