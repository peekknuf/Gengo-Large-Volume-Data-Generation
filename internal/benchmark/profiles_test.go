package benchmark

import (
	"io"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func profileScenario(name string) string {
	return "version: 1\nname: profiles\ndataset: dataset\nschema: gengo_test\ntargets: [direct, shared, dedicated]\nworkload: customer-lookup\nseed: 0\nprotocol: simple\nprofile: " + name + "\nthresholds:\n  max_p95_ms: 100\n  max_error_rate: 0.2\n  min_tps: 0\n"
}

func TestProfileDefaults(t *testing.T) {
	for _, test := range []struct {
		name, warmup, duration, order string
		repeats                       int
		clients                       []int
	}{
		{"quick", "1s", "5s", "fixed", 1, []int{1, 2, 4}},
		{"thorough", "3s", "15s", "rotate", 3, []int{1, 2, 4, 8}},
	} {
		t.Run(test.name, func(t *testing.T) {
			plan, err := LoadScenario(scenarioFixture(t, profileScenario(test.name)))
			if err != nil {
				t.Fatal(err)
			}
			s := plan.Scenario
			if s.ProfileVersion != 1 || s.Warmup != test.warmup || s.Duration != test.duration || *s.Repetitions != test.repeats || s.TargetOrder != test.order || !reflect.DeepEqual(s.Concurrency, test.clients) {
				t.Fatalf("unexpected profile: %+v", s)
			}
			rounds := roundSchedule(s)
			if test.name == "thorough" && !reflect.DeepEqual(rounds[1].Targets, []string{"shared", "dedicated", "direct"}) {
				t.Fatalf("endpoint rotation missing: %+v", rounds)
			}
		})
	}
}

func TestProfileOverridesAndInvalidInputs(t *testing.T) {
	base := profileScenario("thorough")
	plan, err := LoadScenario(scenarioFixture(t, base+"warmup: 0s\nduration: 2s\nrepetitions: 2\ntarget_order: fixed\nconcurrency: [2, 3]\n"))
	if err != nil {
		t.Fatal(err)
	}
	s := plan.Scenario
	if s.Warmup != "0s" || s.Duration != "2s" || *s.Repetitions != 2 || s.TargetOrder != "fixed" || !reflect.DeepEqual(s.Concurrency, []int{2, 3}) {
		t.Fatalf("explicit settings lost: %+v", s)
	}
	for _, extra := range []string{"warmup: ''\n", "duration: ''\n", "repetitions: 0\n", "concurrency: []\n", "concurrency: null\n", "target_order: invalid\n", "profile_version: 2\n"} {
		if _, err := LoadScenario(scenarioFixture(t, base+extra)); err == nil {
			t.Fatalf("accepted invalid explicit override %q", extra)
		}
	}
	if _, err := LoadScenario(scenarioFixture(t, profileScenario("unknown"))); err == nil {
		t.Fatal("accepted unknown profile")
	}
}

func TestProfileArrivalRatesAndBurst(t *testing.T) {
	contents := profileScenario("quick") + "clients: 2\narrival_rates: [1, 2]\nlatency_limit: 1s\n"
	contents = strings.Replace(contents, "min_tps: 0", "min_tps: 0\n  min_rate_attainment: 0.5", 1)
	plan, err := LoadScenario(scenarioFixture(t, contents))
	if err != nil || len(plan.Scenario.Concurrency) != 0 || len(plan.Scenario.ArrivalRates) != 2 {
		t.Fatalf("profile injected concurrency into rate experiment: %v", err)
	}
	if _, err := LoadScenario(scenarioFixture(t, profileScenario("quick")+"connection_mode: burst\nwarmup: 0s\n")); err != nil {
		t.Fatal(err)
	}
}

func TestProfileRunSaveAndCompare(t *testing.T) {
	installClientHelpers(t)
	t.Setenv("GENGO_BENCH_HELPER", "success")
	for _, name := range []string{"quick", "thorough"} {
		plan, err := LoadScenario(scenarioFixture(t, profileScenario(name)+"warmup: 0s\nduration: 2s\nconcurrency: [1]\n"))
		if err != nil {
			t.Fatal(err)
		}
		output := filepath.Join(t.TempDir(), "results")
		result, err := Run(t.Context(), plan, "", output, io.Discard)
		if err != nil {
			t.Fatal(err)
		}
		saved, err := ReadResult(filepath.Join(output, "result.json"))
		if err != nil {
			t.Fatal(err)
		}
		if saved.Scenario.Profile != name || saved.Scenario.ProfileVersion != 1 || saved.Scenario.Duration != "2s" || len(saved.Steps) != *saved.Scenario.Repetitions*3 {
			t.Fatal("profile resolution did not survive saving")
		}
		if _, err := Compare(result, saved); err != nil {
			t.Fatal(err)
		}
		saved.Scenario.ProfileVersion = 2
		if _, err := ReadResult(saveComparisonFixture(t, saved)); err == nil {
			t.Fatal("accepted unsupported saved profile version")
		}
	}
}

func TestCompareProfileUsesResolvedControls(t *testing.T) {
	baseline, candidate := comparisonFixture(t), comparisonFixture(t)
	baseline.Scenario.Profile, baseline.Scenario.ProfileVersion = "quick", 1
	candidate.Scenario.Profile, candidate.Scenario.ProfileVersion = "thorough", 1
	comparison, err := Compare(baseline, candidate)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(strings.Join(comparison.Warnings, " "), "Profile identities differ") {
		t.Fatal("different profile identities were not recorded")
	}
	candidate.Scenario.Duration = "15s"
	if _, err := Compare(baseline, candidate); err == nil {
		t.Fatal("compared profiles with different resolved durations")
	}
}
