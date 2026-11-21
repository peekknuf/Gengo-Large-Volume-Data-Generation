package core

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/peekknuf/Gengo/internal/generation"
)

const SchemaVersion = 1

type RunPlan struct {
	Model  string
	Rows   any
	Format string
	Config generation.Config
}

type DatasetIdentity struct {
	SchemaVersion int              `json:"schema_version"`
	Algorithm     string           `json:"algorithm"`
	Model         string           `json:"model"`
	Seed          uint64           `json:"seed"`
	AsOf          string           `json:"as_of"`
	RowsPerFile   int              `json:"rows_per_file"`
	Counts        map[string]int64 `json:"counts"`
}

type OutputFile struct {
	Name     string `json:"name"`
	Table    string `json:"table"`
	Rows     int64  `json:"rows"`
	Bytes    int64  `json:"bytes"`
	FirstKey int64  `json:"first_key"`
	LastKey  int64  `json:"last_key"`
}

type Manifest struct {
	Version          int             `json:"manifest_version"`
	DatasetID        string          `json:"dataset_id"`
	Identity         DatasetIdentity `json:"identity"`
	Format           string          `json:"format"`
	Complete         bool            `json:"complete"`
	Files            []OutputFile    `json:"files"`
	Workers          int             `json:"workers"`
	GenerationMillis int64           `json:"generation_ms"`
	ValidationMillis int64           `json:"validation_ms"`
}

func (p RunPlan) identity() DatasetIdentity {
	counts := map[string]int64{}
	rows := reflect.ValueOf(p.Rows)
	for i := 0; i < rows.NumField(); i++ {
		field := rowFieldName(rows.Type().Field(i).Name)
		table := "dim_" + field
		switch field {
		case "order_headers":
			table = "fact_orders_header"
		case "order_items", "daily_stock_prices", "appointments", "store_sales", "catalog_sales", "web_sales":
			table = "fact_" + field
		case "store_returns", "catalog_returns", "web_returns", "inventory":
			continue
		}
		counts[table] = rows.Field(i).Int()
	}
	if p.Model == "ecommerce-ds" {
		first := time.Date(p.Config.AsOf.Year()-6, 1, 1, 0, 0, 0, 0, time.UTC)
		last := time.Date(p.Config.AsOf.Year(), 1, 1, 0, 0, 0, 0, time.UTC)
		counts["dim_date"] = int64(last.Sub(first).Hours() / 24)
		counts["dim_time"] = 86400
	}
	return DatasetIdentity{SchemaVersion: SchemaVersion, Algorithm: generation.AlgorithmVersion, Model: p.Model, Seed: p.Config.Seed, AsOf: p.Config.AsOf.Format(time.DateOnly), RowsPerFile: p.Config.RowsPerFile, Counts: counts}
}

func rowFieldName(field string) string {
	var name strings.Builder
	for i, r := range field {
		if i > 0 && r >= 'A' && r <= 'Z' {
			name.WriteByte('_')
		}
		name.WriteRune(r)
	}
	return strings.ToLower(name.String())
}

func writeManifest(p RunPlan, dir string, generationElapsed time.Duration) error {
	start := time.Now()
	identity := p.identity()
	encoded, err := json.Marshal(identity)
	if err != nil {
		return err
	}
	hash := sha256.Sum256(encoded)
	m := Manifest{Version: 1, DatasetID: hex.EncodeToString(hash[:]), Identity: identity, Format: p.Format, Complete: true, Workers: p.Config.Workers, GenerationMillis: generationElapsed.Milliseconds(), Files: []OutputFile{}}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	actual := map[string]int64{}
	for _, entry := range entries {
		if entry.IsDir() {
			return fmt.Errorf("unexpected directory %s in generated data", entry.Name())
		}
		table := strings.TrimSuffix(entry.Name(), "."+p.Format)
		for {
			prefix, suffix, ok := cutLast(table, "_")
			if !ok {
				break
			}
			if _, err := strconv.Atoi(suffix); err != nil {
				break
			}
			table = prefix
		}
		if _, ok := identity.Counts[table]; !ok {
			return fmt.Errorf("unexpected generated file %s", entry.Name())
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		recorded, ok := p.Config.Output.File(entry.Name())
		if !ok {
			return fmt.Errorf("output file %s has no completed writer", entry.Name())
		}
		rows, first, last := recorded.Rows, recorded.FirstKey, recorded.LastKey
		actual[table] += rows
		m.Files = append(m.Files, OutputFile{Name: entry.Name(), Table: table, Rows: rows, Bytes: info.Size(), FirstKey: first, LastKey: last})
	}
	for table, want := range identity.Counts {
		if actual[table] != want {
			return fmt.Errorf("%s generated %d rows, planned %d", table, actual[table], want)
		}
	}
	m.ValidationMillis = time.Since(start).Milliseconds()
	encoded, err = json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, "manifest.json"), append(encoded, '\n'), 0644)
}

func cutLast(s, sep string) (string, string, bool) {
	i := strings.LastIndex(s, sep)
	if i < 0 {
		return s, "", false
	}
	return s[:i], s[i+len(sep):], true
}
