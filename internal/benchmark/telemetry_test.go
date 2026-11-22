package benchmark

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

const metricsProject = "abcdefghijklmnopqrst"
const metricsKey = "sb_secret_test-only"
const metricsText = "# TYPE pg_connections gauge\npg_connections{state=\"active\"} 3\npg_cpu_seconds_total 1.25e+02\n"

func TestConfigureMetrics(t *testing.T) {
	for _, test := range []struct {
		name, mode, key, direct, shared, base, status string
	}{
		{name: "auto secret key", key: metricsKey, status: "collecting"},
		{name: "legacy key", key: "legacy-jwt", status: "collecting"},
		{name: "missing key", status: "disabled"},
		{name: "opt out", mode: "off", key: metricsKey, status: "disabled"},
		{name: "wrong project URL", key: metricsKey, base: "https://elsewhere.example", status: "unavailable"},
		{name: "mixed projects", key: metricsKey, shared: "postgresql://postgres.zyxwvutsrqponmlkjihg@aws-1.pooler.supabase.com/postgres", status: "unavailable"},
		{name: "unknown endpoint", key: metricsKey, direct: "postgresql://postgres@localhost/postgres", status: "unavailable"},
	} {
		t.Run(test.name, func(t *testing.T) {
			settings := map[string]string{
				"SUPABASE_API_KEY": test.key, "SUPABASE_URL": test.base,
				"GENGO_DIRECT_DATABASE_URL":      "postgresql://postgres@db." + metricsProject + ".supabase.co/postgres",
				"GENGO_TRANSACTION_DATABASE_URL": "postgresql://postgres." + metricsProject + "@aws-1.pooler.supabase.com/postgres",
			}
			if test.direct != "" {
				settings["GENGO_DIRECT_DATABASE_URL"] = test.direct
			}
			if test.shared != "" {
				settings["GENGO_TRANSACTION_DATABASE_URL"] = test.shared
			}
			telemetry, source := configureMetrics(Scenario{Targets: []string{"direct", "shared"}, SupabaseMetrics: test.mode}, settings)
			if telemetry.Status != test.status || (source != nil) != (test.status == "collecting") {
				t.Fatalf("status = %s, source present = %t", telemetry.Status, source != nil)
			}
			if source != nil && source.endpoint != "https://"+metricsProject+".supabase.co/customer/v1/privileged/metrics" {
				t.Fatalf("unexpected endpoint %s", source.endpoint)
			}
		})
	}
}

func TestMetricsSettingsPrecedence(t *testing.T) {
	t.Setenv("SUPABASE_API_KEY", metricsKey)
	path := filepath.Join(t.TempDir(), ".env")
	if err := os.WriteFile(path, []byte("SUPABASE_API_KEY=file-key\nSUPABASE_URL=https://project.supabase.co\n"), 0600); err != nil {
		t.Fatal(err)
	}
	settings, err := metricsSettings(path)
	if err != nil || settings["SUPABASE_API_KEY"] != metricsKey {
		t.Fatal("process key must override dotenv")
	}
}

func TestMetricsScrape(t *testing.T) {
	for _, test := range []struct {
		name, body, want string
		code             int
	}{
		{"success", metricsText, "collected", 200},
		{"redacted", "pg_value{key=\"" + metricsKey + "\",auth=\"" + base64.StdEncoding.EncodeToString([]byte("service_role:"+metricsKey)) + "\"} 1\n", "collected", 200},
		{"unauthorized", metricsKey, "unavailable", 401},
		{"server failure", metricsKey, "unavailable", 503},
		{"html", "<html>not metrics</html>", "unavailable", 200},
		{"empty", "# no samples\n", "unavailable", 200},
		{"oversize", strings.Repeat("x", maxMetricsBytes+1), "unavailable", 200},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				user, key, ok := r.BasicAuth()
				if !ok || user != "service_role" || key != metricsKey {
					t.Error("incorrect Basic authentication")
				}
				w.WriteHeader(test.code)
				fmt.Fprint(w, test.body)
			}))
			defer server.Close()
			source := &metricsSource{endpoint: server.URL, key: metricsKey, client: server.Client()}
			output := t.TempDir()
			sample := source.scrape(t.Context(), output, 0)
			if sample.Status != test.want || sample.HTTPStatus != test.code || sample.StartedAt.IsZero() || sample.FinishedAt.Before(sample.StartedAt) {
				t.Fatalf("unexpected scrape: %+v", sample)
			}
			encoded, _ := json.Marshal(sample)
			if bytes.Contains(encoded, []byte(metricsKey)) {
				t.Fatal("credential leaked in metadata")
			}
			if sample.Status == "collected" {
				contents, err := os.ReadFile(filepath.Join(output, sample.File))
				if err != nil || sample.Series < 1 || sample.SHA256 != sqlHash(string(contents)) {
					t.Fatal("snapshot does not match metadata")
				}
				if bytes.Contains(contents, []byte(metricsKey)) {
					t.Fatal("credential leaked in snapshot")
				}
				if test.name == "success" && string(contents) != metricsText {
					t.Fatal("snapshot changed")
				}
			} else if entries, _ := os.ReadDir(output); len(entries) != 0 {
				t.Fatal("saved failed response body")
			}
		})
	}
}

func TestMetricsRejectRedirect(t *testing.T) {
	var followed atomic.Bool
	destination := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { followed.Store(true) }))
	defer destination.Close()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, destination.URL, http.StatusFound) }))
	defer server.Close()
	_, source := configureMetrics(Scenario{Targets: []string{"direct"}}, map[string]string{"SUPABASE_API_KEY": metricsKey, "GENGO_DIRECT_DATABASE_URL": "postgresql://postgres@db." + metricsProject + ".supabase.co/postgres"})
	source.endpoint = server.URL
	sample := source.scrape(t.Context(), t.TempDir(), 0)
	if sample.HTTPStatus != 302 || followed.Load() {
		t.Fatal("metrics request followed redirect")
	}
}

func TestMetricsPeriodicFailureAndShutdown(t *testing.T) {
	var calls atomic.Int32
	second := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			fmt.Fprint(w, metricsText)
			return
		}
		if calls.Load() == 2 {
			close(second)
		}
		w.WriteHeader(503)
	}))
	defer server.Close()
	source := &metricsSource{endpoint: server.URL, key: metricsKey, client: server.Client()}
	telemetry := &SupabaseTelemetry{}
	stop := collectMetrics(t.Context(), source, telemetry, t.TempDir(), 10*time.Millisecond)
	select {
	case <-second:
	case <-time.After(time.Second):
		t.Fatal("no periodic scrape")
	}
	result := stop()
	if result.Status != "partial" || len(result.Samples) < 2 || result.Samples[0].Status != "collected" {
		t.Fatalf("unexpected periodic result: %+v", result)
	}
	if again := stop(); again.Status != "partial" {
		t.Fatal("stop must be idempotent")
	}
}

func TestMetricsCancellation(t *testing.T) {
	started := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { close(started); <-r.Context().Done() }))
	defer server.Close()
	ctx, cancel := context.WithCancel(t.Context())
	go func() { <-started; cancel() }()
	source := &metricsSource{endpoint: server.URL, key: metricsKey, client: server.Client()}
	stop := collectMetrics(ctx, source, &SupabaseTelemetry{}, t.TempDir(), time.Minute)
	if result := stop(); result.Status != "unavailable" || len(result.Samples) != 1 {
		t.Fatalf("unexpected canceled collection: %+v", result)
	}
}

func TestMetricsOptionalRunAndComparison(t *testing.T) {
	installClientHelpers(t)
	t.Setenv("GENGO_BENCH_HELPER", "success")
	t.Setenv("SUPABASE_API_KEY", metricsKey)
	contents := strings.ReplaceAll(validScenario, "max_error_rate: 0.01", "max_error_rate: 0.2")
	for _, mode := range []string{"auto", "off"} {
		plan, err := LoadScenario(scenarioFixture(t, contents+"supabase_metrics: "+mode+"\n"))
		if err != nil {
			t.Fatal(err)
		}
		output := filepath.Join(t.TempDir(), "results")
		result, err := Run(t.Context(), plan, "", output, io.Discard)
		if err != nil {
			t.Fatal(err)
		}
		want := "unavailable"
		if mode == "off" {
			want = "disabled"
		}
		if result.SupabaseMetrics.Status != want || result.Capacity["direct"] != 2 {
			t.Fatalf("telemetry changed benchmark outcome: %+v", result)
		}
		saved, err := ReadResult(filepath.Join(output, "result.json"))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := Compare(saved, saved); err != nil {
			t.Fatal(err)
		}
		for _, step := range saved.Steps {
			if step.Measurement.StartedAt.IsZero() || step.Measurement.FinishedAt.Before(step.Measurement.StartedAt) {
				t.Fatal("missing phase timestamps")
			}
		}
	}
	if _, err := LoadScenario(scenarioFixture(t, contents+"supabase_metrics: invalid\n")); err == nil {
		t.Fatal("accepted invalid telemetry mode")
	}
}
