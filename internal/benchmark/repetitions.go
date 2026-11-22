package benchmark

import (
	"fmt"
	"io"
	"reflect"
	"sort"
	"strings"
)

type Round struct {
	Repetition int      `json:"repetition"`
	Targets    []string `json:"targets"`
}

type Distribution struct {
	Min    float64 `json:"min"`
	Median float64 `json:"median"`
	Max    float64 `json:"max"`
}

type Aggregate struct {
	Target           string        `json:"target"`
	Concurrency      int           `json:"concurrency"`
	ArrivalRate      float64       `json:"arrival_rate,omitempty"`
	Status           string        `json:"status"`
	Expected         int           `json:"expected_repetitions"`
	Measured         int           `json:"measured_repetitions"`
	Passed           int           `json:"passed_repetitions"`
	Failed           int           `json:"failed_repetitions"`
	Errors           int           `json:"errored_repetitions"`
	PassRate         float64       `json:"pass_rate"`
	TPS              *Distribution `json:"tps,omitempty"`
	P95MS            *Distribution `json:"successful_iteration_p95_ms,omitempty"`
	ErrorRate        *Distribution `json:"error_rate,omitempty"`
	SkippedRate      *Distribution `json:"skipped_rate,omitempty"`
	RateAttainment   *Distribution `json:"rate_attainment,omitempty"`
	ConnectionMeanMS *Distribution `json:"average_connection_ms,omitempty"`
}

func repetitions(scenario Scenario) int {
	if scenario.Repetitions == nil {
		return 1
	}
	return *scenario.Repetitions
}

func targetOrder(scenario Scenario) string {
	if scenario.TargetOrder == "" {
		return "fixed"
	}
	return scenario.TargetOrder
}

func minPassRate(scenario Scenario) float64 {
	if scenario.MinPassRate == nil {
		return 1
	}
	return *scenario.MinPassRate
}

func validateRepetitions(scenario Scenario) error {
	count := repetitions(scenario)
	if count < 1 || count > 30 {
		return fmt.Errorf("repetitions must be between 1 and 30")
	}
	if targetOrder(scenario) != "fixed" && targetOrder(scenario) != "rotate" {
		return fmt.Errorf("target_order must be fixed or rotate")
	}
	if !finite(minPassRate(scenario)) || minPassRate(scenario) <= 0 || minPassRate(scenario) > 1 {
		return fmt.Errorf("min_pass_rate must be greater than zero and at most 1")
	}
	if count*len(scenario.Targets)*len(loadLevels(scenario)) > 1024 {
		return fmt.Errorf("experiment exceeds 1024 target/load/repetition samples")
	}
	return nil
}

func roundSchedule(scenario Scenario) []Round {
	var rounds []Round
	for index := 0; index < repetitions(scenario); index++ {
		targets := append([]string(nil), scenario.Targets...)
		if targetOrder(scenario) == "rotate" && len(targets) > 0 {
			shift := index % len(targets)
			targets = append(targets[shift:], targets[:shift]...)
		}
		rounds = append(rounds, Round{Repetition: index + 1, Targets: targets})
	}
	return rounds
}

func sampleRepetition(step Step) int {
	if step.Repetition == 0 {
		return 1
	}
	return step.Repetition
}

func distribution(values []float64) *Distribution {
	if len(values) == 0 {
		return nil
	}
	sort.Float64s(values)
	middle := len(values) / 2
	median := values[middle]
	if len(values)%2 == 0 {
		median = values[middle-1]/2 + values[middle]/2
	}
	return &Distribution{Min: values[0], Median: median, Max: values[len(values)-1]}
}

func aggregateSteps(result *Result) []Aggregate {
	groups := make(map[stepKey][]Step)
	for _, step := range result.Steps {
		key := stepKey{step.Target, step.Concurrency, step.ArrivalRate}
		groups[key] = append(groups[key], step)
	}
	var aggregates []Aggregate
	for _, target := range result.Scenario.Targets {
		info, exists := result.Targets[target]
		if !exists || info.Error != "" {
			continue
		}
		for _, level := range loadLevels(result.Scenario) {
			aggregate := Aggregate{Target: target, Concurrency: level.clients, ArrivalRate: level.rate, Expected: repetitions(result.Scenario), Status: "incomplete"}
			var tps, p95, errors, skipped, attainment, connection []float64
			for _, step := range groups[stepKey{target, level.clients, level.rate}] {
				switch step.Status {
				case "passed":
					aggregate.Passed++
				case "failed":
					aggregate.Failed++
				case "error", "canceled":
					aggregate.Errors++
				default:
					continue
				}
				if step.Measurement == nil || step.Measurement.Status != "completed" || step.Measurement.Metrics == nil {
					continue
				}
				metrics := step.Measurement.Metrics
				aggregate.Measured++
				tps = append(tps, metrics.throughput())
				p95 = append(p95, metrics.P95MS)
				errors = append(errors, metrics.ErrorRate)
				if metrics.SkippedRate != nil {
					skipped = append(skipped, *metrics.SkippedRate)
				}
				if metrics.RateAttainment != nil {
					attainment = append(attainment, *metrics.RateAttainment)
				}
				if metrics.ConnectionMeanMS != nil {
					connection = append(connection, *metrics.ConnectionMeanMS)
				}
			}
			aggregate.PassRate = float64(aggregate.Passed) / float64(aggregate.Expected)
			if aggregate.Errors > 0 {
				aggregate.Status = "error"
			} else if aggregate.Passed+aggregate.Failed == aggregate.Expected {
				aggregate.Status = "failed"
				if aggregate.PassRate >= minPassRate(result.Scenario) {
					aggregate.Status = "passed"
				}
			}
			aggregate.TPS, aggregate.P95MS, aggregate.ErrorRate = distribution(tps), distribution(p95), distribution(errors)
			aggregate.SkippedRate, aggregate.RateAttainment, aggregate.ConnectionMeanMS = distribution(skipped), distribution(attainment), distribution(connection)
			aggregates = append(aggregates, aggregate)
		}
	}
	return aggregates
}

func refreshAggregates(result *Result) {
	result.Aggregates = aggregateSteps(result)
	result.Capacity, result.RateCapacity = nil, nil
	if len(result.Scenario.ArrivalRates) > 0 {
		result.RateCapacity = make(map[string]float64)
	} else {
		result.Capacity = make(map[string]int)
	}
	for _, aggregate := range result.Aggregates {
		if result.RateCapacity != nil {
			if _, exists := result.RateCapacity[aggregate.Target]; !exists {
				result.RateCapacity[aggregate.Target] = 0
			}
			if aggregate.Status == "passed" && aggregate.ArrivalRate > result.RateCapacity[aggregate.Target] {
				result.RateCapacity[aggregate.Target] = aggregate.ArrivalRate
			}
		} else {
			if _, exists := result.Capacity[aggregate.Target]; !exists {
				result.Capacity[aggregate.Target] = 0
			}
			if aggregate.Status == "passed" && aggregate.Concurrency > result.Capacity[aggregate.Target] {
				result.Capacity[aggregate.Target] = aggregate.Concurrency
			}
		}
	}
}

func validateAggregates(result *Result) error {
	if result.Schedule == nil {
		if repetitions(result.Scenario) > 1 || targetOrder(result.Scenario) != "fixed" {
			return fmt.Errorf("repeated or reordered result is missing its schedule")
		}
	} else if !reflect.DeepEqual(result.Schedule, roundSchedule(result.Scenario)) {
		return fmt.Errorf("saved endpoint schedule disagrees with repetition controls")
	}
	computed := *result
	refreshAggregates(&computed)
	if (result.Aggregates == nil && repetitions(result.Scenario) > 1) || (result.Aggregates != nil && !reflect.DeepEqual(result.Aggregates, computed.Aggregates)) {
		return fmt.Errorf("saved repetition aggregates disagree with individual measurements")
	}
	if !reflect.DeepEqual(result.Capacity, computed.Capacity) || !reflect.DeepEqual(result.RateCapacity, computed.RateCapacity) {
		return fmt.Errorf("saved capacity disagrees with passing repeated levels")
	}
	passed := true
	for _, aggregate := range computed.Aggregates {
		if aggregate.Status != "passed" {
			passed = false
		}
	}
	if (result.Status == "completed") != passed {
		return fmt.Errorf("saved experiment status disagrees with repeated pass rule")
	}
	return nil
}

func printRepeatedSummary(writer io.Writer, result *Result) {
	fmt.Fprintf(writer, "Repetitions: %d; endpoint order: %s; required pass rate: %.2f%%\n", repetitions(result.Scenario), targetOrder(result.Scenario), minPassRate(result.Scenario)*100)
	for _, round := range result.Schedule {
		fmt.Fprintf(writer, "Round %d: %s\n", round.Repetition, strings.Join(round.Targets, ", "))
	}
	for _, aggregate := range result.Aggregates {
		fmt.Fprintf(writer, "%s: %d clients", aggregate.Target, aggregate.Concurrency)
		if aggregate.ArrivalRate > 0 {
			fmt.Fprintf(writer, ", %g iterations/s requested", aggregate.ArrivalRate)
		}
		fmt.Fprintf(writer, ", %s, passing repetitions %d/%d, errors %d\n", aggregate.Status, aggregate.Passed, aggregate.Expected, aggregate.Errors)
		if aggregate.TPS == nil {
			fmt.Fprintln(writer, "  No completed measurements")
			continue
		}
		fmt.Fprintf(writer, "  TPS median %.2f [min %.2f, max %.2f]; per-repetition p95 median %.2f ms [min %.2f, max %.2f]\n", aggregate.TPS.Median, aggregate.TPS.Min, aggregate.TPS.Max, aggregate.P95MS.Median, aggregate.P95MS.Min, aggregate.P95MS.Max)
		fmt.Fprintf(writer, "  Error rate median %.2f%% [min %.2f%%, max %.2f%%]\n", aggregate.ErrorRate.Median*100, aggregate.ErrorRate.Min*100, aggregate.ErrorRate.Max*100)
		if aggregate.SkippedRate != nil {
			fmt.Fprintf(writer, "  Skipped rate median %.2f%%; achieved rate median %.2f%% of requested\n", aggregate.SkippedRate.Median*100, aggregate.RateAttainment.Median*100)
		}
		if aggregate.ConnectionMeanMS != nil {
			fmt.Fprintf(writer, "  Per-repetition mean connection time median %.2f ms [min %.2f, max %.2f]\n", aggregate.ConnectionMeanMS.Median, aggregate.ConnectionMeanMS.Min, aggregate.ConnectionMeanMS.Max)
		}
	}
}
