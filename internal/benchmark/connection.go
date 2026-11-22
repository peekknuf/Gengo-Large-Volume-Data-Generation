package benchmark

import (
	"fmt"
	"time"
)

func connectionMode(scenario Scenario) string {
	if scenario.ConnectionMode == "" {
		return "persistent"
	}
	return scenario.ConnectionMode
}

func validateConnection(scenario Scenario) error {
	if !finite(scenario.Thresholds.MaxConnectionMeanMS) || scenario.Thresholds.MaxConnectionMeanMS < 0 || (connectionMode(scenario) == "persistent" && scenario.Thresholds.MaxConnectionMeanMS != 0) {
		return fmt.Errorf("max_connection_mean_ms must be nonnegative and requires reconnect or burst mode")
	}
	if scenario.Protocol != "simple" && scenario.Protocol != "extended" && scenario.Protocol != "prepared" {
		return fmt.Errorf("protocol must be simple, extended, or prepared")
	}
	switch connectionMode(scenario) {
	case "persistent", "reconnect":
	case "burst":
		warmup, err := time.ParseDuration(scenario.Warmup)
		if err != nil || warmup != 0 {
			return fmt.Errorf("connection bursts require warmup: 0s")
		}
		if len(scenario.ArrivalRates) > 0 {
			return fmt.Errorf("connection bursts use concurrency sizes, not arrival rates")
		}
		for _, clients := range scenario.Concurrency {
			if clients > 128 {
				return fmt.Errorf("connection bursts support at most 128 clients, with one thread per client")
			}
		}
	default:
		return fmt.Errorf("connection_mode must be persistent, reconnect, or burst")
	}
	return nil
}

func (metrics *Metrics) throughput() float64 {
	if metrics.TPSIncludingConnections != nil {
		return *metrics.TPSIncludingConnections
	}
	return metrics.TPS
}

func validateConnectionMetrics(metrics *Metrics, scenario Scenario, clients int) error {
	mode := connectionMode(scenario)
	if mode == "persistent" {
		if metrics.TPSIncludingConnections != nil || metrics.ConnectionMeanMS != nil || metrics.SecondsIncludingConnections != nil || metrics.ThroughputBasis != "" {
			return fmt.Errorf("persistent measurements contain reconnect timing metadata")
		}
		return nil
	}
	if metrics.TPSIncludingConnections == nil || metrics.ConnectionMeanMS == nil || metrics.SecondsIncludingConnections == nil || metrics.TPS != 0 || metrics.ActiveSeconds != 0 || metrics.InitialConnectionMS != 0 || metrics.ThroughputBasis != "including-connection-times" {
		return fmt.Errorf("connection experiment is missing or has conflicting timing metadata")
	}
	if !finite(*metrics.ConnectionMeanMS) || *metrics.ConnectionMeanMS < 0 || !finite(*metrics.SecondsIncludingConnections) || *metrics.SecondsIncludingConnections <= 0 {
		return fmt.Errorf("invalid connection timing measurements")
	}
	if mode == "burst" && (metrics.RequestedIterations == nil || *metrics.RequestedIterations != int64(clients) || metrics.CompletedIterations+metrics.FailedIterations != int64(clients)) {
		return fmt.Errorf("connection burst counts do not match one iteration per client")
	}
	return nil
}
