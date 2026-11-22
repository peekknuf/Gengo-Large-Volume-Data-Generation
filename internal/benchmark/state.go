package benchmark

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/peekknuf/Gengo/internal/postgres"
)

func accessMode(scenario Scenario) string {
	if scenario.AccessMode == "" {
		return "read-only"
	}
	return scenario.AccessMode
}

func statePolicy(scenario Scenario) string {
	if scenario.StatePolicy == "" {
		return "read-only-reuse"
	}
	return scenario.StatePolicy
}

func validateStatePolicy(scenario Scenario) error {
	mode, policy := accessMode(scenario), statePolicy(scenario)
	if mode != "read-only" && mode != "read-write" {
		return fmt.Errorf("access_mode must be read-only or read-write")
	}
	if scenario.Workload != "custom-sql" && (mode != "read-only" || policy != "read-only-reuse" || scenario.SetupFile != "" || scenario.ResetFile != "") {
		return fmt.Errorf("write access and setup/reset policies require custom-sql")
	}
	switch policy {
	case "read-only-reuse":
		if mode != "read-only" || scenario.SetupFile != "" || scenario.ResetFile != "" {
			return fmt.Errorf("read-only-reuse requires read-only access without setup/reset files")
		}
	case "reuse":
		if scenario.ResetFile != "" {
			return fmt.Errorf("reuse does not accept reset_file")
		}
	case "reset-per-phase":
		if scenario.ResetFile == "" {
			return fmt.Errorf("reset-per-phase requires reset_file")
		}
	default:
		return fmt.Errorf("state_policy must be read-only-reuse, reuse, or reset-per-phase")
	}
	return nil
}

func sqlHash(sql string) string {
	if sql == "" {
		return ""
	}
	hash := sha256.Sum256([]byte(sql))
	return hex.EncodeToString(hash[:])
}

func completedStatePhase(phase *Phase) bool {
	return phase != nil && phase.Status == "completed" && phase.Error == "" && phase.Metrics == nil && finite(phase.ElapsedSeconds) && phase.ElapsedSeconds >= 0
}

func validateSavedState(result *Result) error {
	if err := validateStatePolicy(result.Scenario); err != nil {
		return err
	}
	if result.StartingState != statePolicy(result.Scenario) {
		return fmt.Errorf("saved starting state disagrees with scenario policy")
	}
	for _, script := range []struct{ path, hash string }{{result.Scenario.SetupFile, result.SetupSHA256}, {result.Scenario.ResetFile, result.ResetSHA256}} {
		if script.path == "" {
			if script.hash != "" {
				return fmt.Errorf("saved lifecycle hash has no script path")
			}
			continue
		}
		hash, err := hex.DecodeString(script.hash)
		if err != nil || len(hash) != sha256.Size || script.hash != strings.ToLower(script.hash) {
			return fmt.Errorf("saved setup/reset script requires a valid SHA-256 identity")
		}
	}
	for _, step := range result.Steps {
		if result.StartingState == "reset-per-phase" {
			if !completedStatePhase(step.ResetBeforeMeasurement) || !completedStatePhase(step.ResetAfter) || (step.Warmup != nil && !completedStatePhase(step.ResetBeforeWarmup)) || (step.Warmup == nil && step.ResetBeforeWarmup != nil) {
				return fmt.Errorf("cannot compare missing, errored, or unexpected reset phases")
			}
		} else if step.ResetBeforeWarmup != nil || step.ResetBeforeMeasurement != nil || step.ResetAfter != nil {
			return fmt.Errorf("saved reset phases disagree with state policy")
		}
	}
	return nil
}

func runStateScript(ctx context.Context, client *postgres.Client, script, directory string) *Phase {
	phase := &Phase{Status: "error", StartedAt: time.Now().UTC()}
	defer func() { phase.FinishedAt = time.Now().UTC() }()
	if err := os.Mkdir(directory, 0755); err != nil {
		phase.Error = err.Error()
		return phase
	}
	phaseContext, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	var output, diagnostics bytes.Buffer
	start := time.Now()
	err := client.Run(phaseContext, []string{"--single-transaction", "--file", script}, &output, &diagnostics)
	phase.ElapsedSeconds = time.Since(start).Seconds()
	if writeErr := os.WriteFile(filepath.Join(directory, "psql.txt"), []byte(output.String()+"\n"+diagnostics.String()), 0644); writeErr != nil {
		phase.Error = writeErr.Error()
		return phase
	}
	if err != nil {
		phase.Error = err.Error() + "; " + strings.TrimSpace(diagnostics.String())
		return phase
	}
	phase.Status = "completed"
	return phase
}
