package benchmark

import (
	"bufio"
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/joho/godotenv"
)

const metricsInterval = time.Minute
const maxMetricsBytes = 8 << 20

// SupabaseTelemetry contains project-wide observations, not per-target query timings.
type SupabaseTelemetry struct {
	Status          string          `json:"status"`
	Reason          string          `json:"reason,omitempty"`
	ProjectRef      string          `json:"project_ref,omitempty"`
	IntervalSeconds int             `json:"interval_seconds"`
	Scope           string          `json:"scope"`
	Samples         []MetricsSample `json:"samples"`
}

type MetricsSample struct {
	StartedAt  time.Time `json:"started_at"`
	FinishedAt time.Time `json:"finished_at"`
	Status     string    `json:"status"`
	HTTPStatus int       `json:"http_status,omitempty"`
	File       string    `json:"file,omitempty"`
	SHA256     string    `json:"sha256,omitempty"`
	Series     int       `json:"series,omitempty"`
	Error      string    `json:"error,omitempty"`
}

type metricsSource struct {
	endpoint string
	key      string
	client   *http.Client
}

func metricsSettings(envFile string) (map[string]string, error) {
	settings := map[string]string{}
	if envFile != "" {
		values, err := godotenv.Read(envFile)
		if err != nil {
			return nil, fmt.Errorf("cannot read metrics environment file")
		}
		for key, value := range values {
			settings[strings.ToUpper(key)] = value
		}
	}
	for _, entry := range os.Environ() {
		key, value, _ := strings.Cut(entry, "=")
		settings[strings.ToUpper(key)] = value
	}
	return settings, nil
}

func configureMetrics(scenario Scenario, settings map[string]string) (*SupabaseTelemetry, *metricsSource) {
	telemetry := &SupabaseTelemetry{Status: "disabled", IntervalSeconds: 60, Scope: "project", Samples: []MetricsSample{}}
	if scenario.SupabaseMetrics == "off" {
		telemetry.Reason = "disabled by scenario"
		return telemetry, nil
	}
	key := settings["SUPABASE_API_KEY"]
	if key == "" {
		telemetry.Reason = "SUPABASE_API_KEY is not configured"
		return telemetry, nil
	}
	telemetry.Status = "unavailable"
	// Infer every selected endpoint so one project's key is never sent for another experiment.
	ref := ""
	for _, target := range scenario.Targets {
		variable := map[string]string{"direct": "GENGO_DIRECT_DATABASE_URL", "shared": "GENGO_TRANSACTION_DATABASE_URL", "dedicated": "GENGO_DEDICATED_DATABASE_URL"}[target]
		host, user := settings["PGHOST"], settings["PGUSER"]
		if target != "libpq" {
			parsed, err := url.Parse(settings[variable])
			if err != nil || parsed == nil {
				telemetry.Reason = "cannot identify the metrics project from database targets"
				return telemetry, nil
			}
			host = parsed.Hostname()
			if parsed.User != nil {
				user = parsed.User.Username()
			}
		}
		candidate := ""
		if strings.HasPrefix(host, "db.") && strings.HasSuffix(host, ".supabase.co") {
			candidate = strings.TrimSuffix(strings.TrimPrefix(host, "db."), ".supabase.co")
		} else if strings.HasSuffix(host, ".pooler.supabase.com") {
			_, candidate, _ = strings.Cut(user, ".")
		}
		if !regexp.MustCompile(`^[a-z0-9]{20}$`).MatchString(candidate) || (ref != "" && candidate != ref) {
			telemetry.Reason = "metrics require all database targets to identify the same hosted Supabase project"
			return telemetry, nil
		}
		ref = candidate
	}
	if ref == "" {
		telemetry.Reason = "no Supabase project identified"
		return telemetry, nil
	}
	base := "https://" + ref + ".supabase.co"
	if configured := settings["SUPABASE_URL"]; configured != "" && strings.TrimRight(configured, "/") != base {
		telemetry.Reason = "SUPABASE_URL does not match the benchmark database project"
		return telemetry, nil
	}
	telemetry.ProjectRef = ref
	telemetry.Status = "collecting"
	return telemetry, &metricsSource{endpoint: base + "/customer/v1/privileged/metrics", key: key, client: &http.Client{
		Timeout:       10 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}}
}

func (source *metricsSource) scrape(ctx context.Context, output string, index int) (sample MetricsSample) {
	sample = MetricsSample{StartedAt: time.Now().UTC(), Status: "unavailable"}
	defer func() { sample.FinishedAt = time.Now().UTC() }()
	// Do not retain transport errors or response bodies on failure; they may echo credentials.
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, source.endpoint, nil)
	if err != nil {
		sample.Error = "cannot create metrics request"
		return
	}
	request.SetBasicAuth("service_role", source.key)
	request.Header.Set("Accept", "text/plain")
	response, err := source.client.Do(request)
	if err != nil {
		sample.Error = "metrics request failed or was canceled"
		return
	}
	sample.HTTPStatus = response.StatusCode
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		sample.Error = fmt.Sprintf("metrics endpoint returned HTTP %d", response.StatusCode)
		return
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, maxMetricsBytes+1))
	if err != nil || len(body) > maxMetricsBytes {
		sample.Error = "cannot read metrics response within the 8 MiB limit"
		return
	}
	contents := strings.ReplaceAll(string(body), source.key, "[REDACTED]")
	contents = strings.ReplaceAll(contents, base64.StdEncoding.EncodeToString([]byte("service_role:"+source.key)), "[REDACTED]")
	series := countMetrics(contents)
	if series == 0 {
		sample.Error = "metrics response contains no valid Prometheus samples"
		return
	}
	name := fmt.Sprintf("supabase-metrics-%06d.prom", index)
	if err := os.WriteFile(filepath.Join(output, name), []byte(contents), 0600); err != nil {
		sample.Error = "cannot save metrics snapshot"
		return
	}
	sample.Status, sample.File, sample.SHA256, sample.Series = "collected", name, sqlHash(contents), series
	return
}

var prometheusSample = regexp.MustCompile(`^[a-zA-Z_:][a-zA-Z0-9_:]*(\{.*\})?\s+([-+]?(?:[0-9]*\.?[0-9]+(?:[eE][-+]?[0-9]+)?|Inf)|NaN)(?:\s+[0-9]+)?\s*$`)

func countMetrics(contents string) int {
	scanner := bufio.NewScanner(strings.NewReader(contents))
	scanner.Buffer(make([]byte, 4096), maxMetricsBytes)
	count := 0
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if !prometheusSample.MatchString(line) {
			return 0
		}
		count++
	}
	if scanner.Err() != nil {
		return 0
	}
	return count
}

func startMetrics(ctx context.Context, scenario Scenario, envFile, output string) func() *SupabaseTelemetry {
	settings, err := metricsSettings(envFile)
	if err != nil {
		return func() *SupabaseTelemetry {
			return &SupabaseTelemetry{Status: "unavailable", Reason: err.Error(), IntervalSeconds: 60, Scope: "project", Samples: []MetricsSample{}}
		}
	}
	telemetry, source := configureMetrics(scenario, settings)
	if source == nil {
		return func() *SupabaseTelemetry { return telemetry }
	}
	return collectMetrics(ctx, source, telemetry, output, metricsInterval)
}

func collectMetrics(ctx context.Context, source *metricsSource, telemetry *SupabaseTelemetry, output string, interval time.Duration) func() *SupabaseTelemetry {
	ctx, cancel := context.WithCancel(ctx)
	telemetry.Samples = append(telemetry.Samples, source.scrape(ctx, output, 0))
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if ctx.Err() != nil {
					return
				}
				telemetry.Samples = append(telemetry.Samples, source.scrape(ctx, output, len(telemetry.Samples)))
			}
		}
	}()
	return func() *SupabaseTelemetry {
		cancel()
		<-done
		collected := 0
		for _, sample := range telemetry.Samples {
			if sample.Status == "collected" {
				collected++
			}
		}
		telemetry.Status = "available"
		if collected == 0 {
			telemetry.Status, telemetry.Reason = "unavailable", "no metrics snapshots collected"
		} else if collected < len(telemetry.Samples) {
			telemetry.Status, telemetry.Reason = "partial", "one or more metrics scrapes failed"
		}
		return telemetry
	}
}
