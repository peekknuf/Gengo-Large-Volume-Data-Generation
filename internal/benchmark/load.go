package benchmark

import (
	"fmt"
	"math"
	"strconv"
	"time"
)

type loadLevel struct {
	clients int
	rate    float64
}

func validateRateMetrics(metrics *Metrics, rate float64) error {
	if rate == 0 {
		if metrics.SkippedIterations != 0 || metrics.RequestedIterations != nil || metrics.RateAttainment != nil || metrics.SkippedRate != nil || metrics.ScheduleLagMeanMS != nil || metrics.ScheduleLagMaxMS != nil || metrics.LatencyBasis != "" {
			return fmt.Errorf("concurrency measurements contain unexpected arrival-rate data")
		}
		return nil
	}
	if metrics.SkippedIterations < 0 || metrics.SkippedIterations > math.MaxInt64-metrics.CompletedIterations-metrics.FailedIterations || metrics.RequestedIterations == nil || metrics.RateAttainment == nil || metrics.SkippedRate == nil || metrics.ScheduleLagMeanMS == nil || metrics.ScheduleLagMaxMS == nil || metrics.LatencyBasis != "scheduled-start-to-completion" {
		return fmt.Errorf("arrival-rate measurement is missing or has invalid scheduled-work metadata")
	}
	requested := metrics.CompletedIterations + metrics.FailedIterations + metrics.SkippedIterations
	if *metrics.RequestedIterations != requested || !finite(*metrics.RateAttainment) || *metrics.RateAttainment <= 0 || math.Abs(*metrics.RateAttainment-metrics.TPS/rate) > 1e-9 || !finite(*metrics.SkippedRate) || *metrics.SkippedRate < 0 || *metrics.SkippedRate > 1 || math.Abs(*metrics.SkippedRate-float64(metrics.SkippedIterations)/float64(requested)) > 1e-12 {
		return fmt.Errorf("saved arrival-rate measurements disagree with counts or requested rate")
	}
	if !finite(*metrics.ScheduleLagMeanMS) || !finite(*metrics.ScheduleLagMaxMS) || *metrics.ScheduleLagMeanMS < 0 || *metrics.ScheduleLagMaxMS < *metrics.ScheduleLagMeanMS || !finite(metrics.MeanMS) || metrics.MeanMS < *metrics.ScheduleLagMeanMS {
		return fmt.Errorf("invalid arrival-rate schedule lag measurements")
	}
	return nil
}

func loadModel(scenario Scenario) string {
	if len(scenario.ArrivalRates) > 0 {
		return "poisson-arrival-rate"
	}
	return "closed-loop-concurrency"
}

func validateLoad(scenario Scenario) error {
	limits := scenario.Thresholds
	if !finite(limits.MaxSkippedRate) || limits.MaxSkippedRate < 0 || limits.MaxSkippedRate > 1 || !finite(limits.MinRateAttainment) || limits.MinRateAttainment < 0 || limits.MinRateAttainment > 1 {
		return fmt.Errorf("max_skipped_rate and min_rate_attainment must be between 0 and 1")
	}
	if len(scenario.ArrivalRates) > 0 {
		if len(scenario.Concurrency) != 0 || scenario.Clients < 1 || scenario.Clients > 1024 || len(scenario.ArrivalRates) > 32 {
			return fmt.Errorf("arrival_rates requires 1 to 32 rates, clients between 1 and 1024, and no concurrency sweep")
		}
		previous := float64(0)
		for _, rate := range scenario.ArrivalRates {
			if !finite(rate) || rate < 0.1 || rate <= previous || rate > 1_000_000 {
				return fmt.Errorf("arrival_rates must increase and stay between 0.1 and 1000000 iterations per second")
			}
			previous = rate
		}
		limit, err := time.ParseDuration(scenario.LatencyLimit)
		if err != nil || limit < time.Millisecond || limit > 30*time.Second {
			return fmt.Errorf("arrival_rates requires latency_limit between 1ms and 30s")
		}
		if limits.MinRateAttainment <= 0 {
			return fmt.Errorf("arrival_rates requires positive min_rate_attainment, at most 1")
		}
		return nil
	}
	if scenario.Clients != 0 || scenario.LatencyLimit != "" || limits.MaxSkippedRate != 0 || limits.MinRateAttainment != 0 {
		return fmt.Errorf("clients, latency_limit, and rate thresholds require arrival_rates")
	}
	if len(scenario.Concurrency) == 0 || len(scenario.Concurrency) > 32 {
		return fmt.Errorf("concurrency requires 1 to 32 increasing load levels")
	}
	previous := 0
	for _, clients := range scenario.Concurrency {
		if clients <= previous || clients > 1024 {
			return fmt.Errorf("concurrency levels must increase and stay between 1 and 1024")
		}
		previous = clients
	}
	return nil
}

func loadLevels(scenario Scenario) []loadLevel {
	var levels []loadLevel
	if len(scenario.ArrivalRates) > 0 {
		for _, rate := range scenario.ArrivalRates {
			levels = append(levels, loadLevel{clients: scenario.Clients, rate: rate})
		}
	} else {
		for _, clients := range scenario.Concurrency {
			levels = append(levels, loadLevel{clients: clients})
		}
	}
	return levels
}

func (level loadLevel) label() string {
	label := "c" + strconv.Itoa(level.clients)
	if level.rate > 0 {
		label += "-r" + strconv.FormatFloat(level.rate, 'f', -1, 64)
	}
	return label
}
