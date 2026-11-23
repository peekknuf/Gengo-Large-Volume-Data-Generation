package benchmark

import (
	"fmt"

	"gopkg.in/yaml.v3"
)

// ProfileVersion changes when the named profiles' defaults change.
const ProfileVersion = 1

func resolveProfile(scenario *Scenario, fields map[string]yaml.Node) error {
	if scenario.Profile == "" {
		return nil
	}
	var warmup, duration, order string
	var repetitions int
	var concurrency []int
	switch scenario.Profile {
	case "quick":
		warmup, duration, order, repetitions = "1s", "5s", "fixed", 1
		concurrency = []int{1, 2, 4}
	case "thorough":
		warmup, duration, order, repetitions = "3s", "15s", "rotate", 3
		concurrency = []int{1, 2, 4, 8}
	default:
		return fmt.Errorf("profile must be quick or thorough")
	}
	scenario.ProfileVersion = ProfileVersion
	if _, present := fields["warmup"]; !present {
		scenario.Warmup = warmup
	}
	if _, present := fields["duration"]; !present {
		scenario.Duration = duration
	}
	if _, present := fields["target_order"]; !present {
		scenario.TargetOrder = order
	}
	if _, present := fields["repetitions"]; !present {
		scenario.Repetitions = &repetitions
	}
	_, hasConcurrency := fields["concurrency"]
	_, hasRates := fields["arrival_rates"]
	if !hasConcurrency && !hasRates {
		scenario.Concurrency = concurrency
	}
	return nil
}

func validateSavedProfile(scenario Scenario) error {
	if scenario.Profile == "" {
		if scenario.ProfileVersion != 0 {
			return fmt.Errorf("saved profile version has no profile name")
		}
		return nil
	}
	if (scenario.Profile != "quick" && scenario.Profile != "thorough") || scenario.ProfileVersion != ProfileVersion {
		return fmt.Errorf("unsupported saved experiment profile or profile version")
	}
	return nil
}
