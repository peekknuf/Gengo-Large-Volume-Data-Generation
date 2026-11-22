package benchmark

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/peekknuf/Gengo/internal/core"
	"gopkg.in/yaml.v3"
)

type Thresholds struct {
	MaxP95MS     float64 `yaml:"max_p95_ms" json:"max_p95_ms"`
	MaxErrorRate float64 `yaml:"max_error_rate" json:"max_error_rate"`
	MinTPS       float64 `yaml:"min_tps" json:"min_tps"`
}

type Scenario struct {
	Version     int        `yaml:"version" json:"version"`
	Name        string     `yaml:"name" json:"name"`
	Dataset     string     `yaml:"dataset" json:"dataset"`
	Schema      string     `yaml:"schema" json:"schema"`
	Targets     []string   `yaml:"targets" json:"targets"`
	Workload    string     `yaml:"workload" json:"workload"`
	Seed        *uint64    `yaml:"seed" json:"seed"`
	Protocol    string     `yaml:"protocol" json:"protocol"`
	Concurrency []int      `yaml:"concurrency" json:"concurrency"`
	Warmup      string     `yaml:"warmup" json:"warmup"`
	Duration    string     `yaml:"duration" json:"duration"`
	Thresholds  Thresholds `yaml:"thresholds" json:"thresholds"`
}

type Plan struct {
	Scenario Scenario
	Dataset  core.Manifest
	Warmup   time.Duration
	Duration time.Duration
}

func LoadScenario(path string) (Plan, error) {
	file, err := os.Open(path)
	if err != nil {
		return Plan{}, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || info.Size() > 1<<20 {
		return Plan{}, fmt.Errorf("scenario must be at most 1 MiB")
	}
	decoder := yaml.NewDecoder(io.LimitReader(file, 1<<20))
	decoder.KnownFields(true)
	var scenario Scenario
	if err := decoder.Decode(&scenario); err != nil {
		return Plan{}, fmt.Errorf("invalid scenario YAML; check field names, types, and syntax")
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return Plan{}, fmt.Errorf("scenario must contain one YAML document")
	}
	if scenario.Version != 1 || strings.TrimSpace(scenario.Name) == "" || len(scenario.Name) > 100 {
		return Plan{}, fmt.Errorf("scenario requires version 1 and a name of 1 to 100 characters")
	}
	if !regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,62}$`).MatchString(scenario.Schema) {
		return Plan{}, fmt.Errorf("schema must be an ASCII PostgreSQL identifier of 1 to 63 characters")
	}
	if scenario.Workload != "customer-lookup" || scenario.Protocol != "simple" {
		return Plan{}, fmt.Errorf("version 1 supports workload customer-lookup and protocol simple")
	}
	if scenario.Seed == nil || *scenario.Seed > math.MaxInt64 {
		return Plan{}, fmt.Errorf("seed must be explicitly set between 0 and 9223372036854775807")
	}
	seen := map[string]bool{}
	for _, target := range scenario.Targets {
		if (target != "direct" && target != "shared" && target != "dedicated" && target != "libpq") || seen[target] {
			return Plan{}, fmt.Errorf("targets must be distinct direct, shared, dedicated, or libpq entries")
		}
		seen[target] = true
	}
	if len(scenario.Targets) == 0 {
		return Plan{}, fmt.Errorf("at least one target is required")
	}
	if len(scenario.Concurrency) == 0 || len(scenario.Concurrency) > 32 {
		return Plan{}, fmt.Errorf("concurrency requires 1 to 32 increasing load levels")
	}
	previous := 0
	for _, clients := range scenario.Concurrency {
		if clients <= previous || clients > 1024 {
			return Plan{}, fmt.Errorf("concurrency levels must increase and stay between 1 and 1024")
		}
		previous = clients
	}
	warmup, err := time.ParseDuration(scenario.Warmup)
	if err != nil || warmup < 0 || warmup > 5*time.Minute || warmup%time.Second != 0 {
		return Plan{}, fmt.Errorf("warmup must be explicitly set to whole seconds between 0s and 5m")
	}
	duration, err := time.ParseDuration(scenario.Duration)
	if err != nil || duration < time.Second || duration > 5*time.Minute || duration%time.Second != 0 {
		return Plan{}, fmt.Errorf("duration must be whole seconds between 1s and 5m")
	}
	limits := scenario.Thresholds
	if !finite(limits.MaxP95MS) || limits.MaxP95MS <= 0 || !finite(limits.MaxErrorRate) || limits.MaxErrorRate < 0 || limits.MaxErrorRate > 1 || !finite(limits.MinTPS) || limits.MinTPS < 0 {
		return Plan{}, fmt.Errorf("thresholds require positive max_p95_ms, max_error_rate between 0 and 1, and nonnegative min_tps")
	}
	if scenario.Dataset == "" {
		return Plan{}, fmt.Errorf("dataset directory is required")
	}
	if !filepath.IsAbs(scenario.Dataset) {
		scenario.Dataset = filepath.Join(filepath.Dir(path), scenario.Dataset)
	}
	scenario.Dataset, err = filepath.Abs(scenario.Dataset)
	if err != nil {
		return Plan{}, err
	}
	manifestFile, err := os.Open(filepath.Join(scenario.Dataset, "manifest.json"))
	if err != nil {
		return Plan{}, fmt.Errorf("read dataset manifest: %w", err)
	}
	defer manifestFile.Close()
	manifestInfo, err := manifestFile.Stat()
	if err != nil || manifestInfo.Size() > 4<<20 {
		return Plan{}, fmt.Errorf("manifest must be at most 4 MiB")
	}
	var manifest core.Manifest
	manifestDecoder := json.NewDecoder(io.LimitReader(manifestFile, 4<<20))
	if err := manifestDecoder.Decode(&manifest); err != nil {
		return Plan{}, fmt.Errorf("invalid dataset manifest")
	}
	if err := manifestDecoder.Decode(&extra); err != io.EOF {
		return Plan{}, fmt.Errorf("manifest must contain one JSON object")
	}
	identity, err := json.Marshal(manifest.Identity)
	if err != nil {
		return Plan{}, err
	}
	hash := sha256.Sum256(identity)
	if manifest.Version != 1 || !manifest.Complete || manifest.Identity.SchemaVersion != core.SchemaVersion || manifest.Identity.Model != "ecommerce" || manifest.Identity.Counts["dim_customers"] < 1 || manifest.DatasetID != hex.EncodeToString(hash[:]) {
		return Plan{}, fmt.Errorf("expected a completed ecommerce manifest with valid identity and positive customer count")
	}
	return Plan{Scenario: scenario, Dataset: manifest, Warmup: warmup, Duration: duration}, nil
}

func finite(value float64) bool {
	return !math.IsNaN(value) && !math.IsInf(value, 0)
}
