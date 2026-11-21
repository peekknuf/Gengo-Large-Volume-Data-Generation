package core

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func loadFixture(t *testing.T) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "Max's dataset")
	counts := ECommerceRowCounts{Customers: 3, CustomerAddresses: 5, Suppliers: 2, Products: 3, ProductCategories: 10, OrderHeaders: 7, OrderItems: 39}
	if err := GenerateModelData("ecommerce", counts, "csv", dir, fixedConfig(t, 0, 2, 4)); err != nil {
		t.Fatal(err)
	}
	return dir
}

func TestCSVLoadPlan(t *testing.T) {
	dataset := loadFixture(t)
	destination := filepath.Join(t.TempDir(), "import")
	if err := WriteCSVLoadPlan(dataset, "gengo_test", destination); err != nil {
		t.Fatal(err)
	}
	read := func(name string) string {
		t.Helper()
		contents, err := os.ReadFile(filepath.Join(destination, name))
		if err != nil {
			t.Fatal(err)
		}
		return string(contents)
	}
	ddl := read("schema.sql")
	for _, want := range []string{`CREATE SCHEMA "gengo_test";`, `"order_timestamp_unix" bigint NOT NULL`, `"base_price" double precision NOT NULL`} {
		if !strings.Contains(ddl, want) {
			t.Fatalf("DDL missing %s", want)
		}
	}
	load := read("load.sql")
	if strings.Count(load, "\\copy ") != 9 {
		t.Fatalf("expected all 9 dimension and fact shards, got\n%s", load)
	}
	if !strings.Contains(load, "Max''s dataset/") || !strings.Contains(load, "FORCE_NOT_NULL") || !strings.HasSuffix(load, "\\ir verify.sql\nCOMMIT;\n") {
		t.Fatalf("invalid import paths, text handling, or count validation\n%s", load)
	}
	if strings.Count(read("indexes.sql"), "PRIMARY KEY") != 7 {
		t.Fatal("missing primary keys")
	}
	if strings.Count(read("foreign_keys.sql"), "NOT VALID") != 8 {
		t.Fatal("missing foreign keys")
	}
	if strings.Count(read("validate_foreign_keys.sql"), "VALIDATE CONSTRAINT") != 8 {
		t.Fatal("missing FK validation")
	}
	if strings.Count(read("analyze.sql"), "ANALYZE ") != 7 {
		t.Fatal("missing statistics collection")
	}
	if !strings.Contains(read("verify.sql"), "IF actual <> 39 THEN") {
		t.Fatal("missing declared row count")
	}
	run := read("run.psql")
	if !strings.Contains(run, "\\set ON_ERROR_STOP on") || strings.Count(run, "\\ir ") != 6 {
		t.Fatal("invalid stage runner")
	}
	if err := WriteCSVLoadPlan(dataset, "gengo_test", destination); err == nil {
		t.Fatal("overwrote an existing load plan")
	}
	if !strings.Contains(read("schema.sql"), `CREATE SCHEMA "gengo_test";`) {
		t.Fatal("failed retry damaged the plan")
	}
}

func TestCSVLoadPlanRejectsInvalidSources(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*testing.T, string, *Manifest)
	}{
		{"incomplete", func(_ *testing.T, _ string, m *Manifest) { m.Complete = false }},
		{"unsupported_format", func(_ *testing.T, _ string, m *Manifest) { m.Format = "parquet" }},
		{"unsupported_model", func(_ *testing.T, _ string, m *Manifest) { m.Identity.Model = "medical" }},
		{"unknown_schema", func(_ *testing.T, _ string, m *Manifest) { m.Identity.SchemaVersion = 99 }},
		{"missing_file", func(t *testing.T, dir string, m *Manifest) {
			if err := os.Remove(filepath.Join(dir, m.Files[0].Name)); err != nil {
				t.Fatal(err)
			}
		}},
		{"changed_size", func(_ *testing.T, _ string, m *Manifest) { m.Files[0].Bytes++ }},
		{"duplicate_file", func(_ *testing.T, _ string, m *Manifest) { m.Files = append(m.Files, m.Files[0]) }},
		{"traversal", func(_ *testing.T, _ string, m *Manifest) { m.Files[0].Name = "../outside.csv" }},
		{"wrong_rows", func(_ *testing.T, _ string, m *Manifest) { m.Files[0].Rows++ }},
		{"wrong_header", func(t *testing.T, dir string, m *Manifest) {
			path := filepath.Join(dir, m.Files[0].Name)
			contents, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			contents[0] = 'x'
			if err = os.WriteFile(path, contents, 0644); err != nil {
				t.Fatal(err)
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := loadFixture(t)
			contents, err := os.ReadFile(filepath.Join(dir, "manifest.json"))
			if err != nil {
				t.Fatal(err)
			}
			var manifest Manifest
			if err = json.Unmarshal(contents, &manifest); err != nil {
				t.Fatal(err)
			}
			tc.mutate(t, dir, &manifest)
			contents, err = json.Marshal(manifest)
			if err != nil {
				t.Fatal(err)
			}
			if err = os.WriteFile(filepath.Join(dir, "manifest.json"), contents, 0644); err != nil {
				t.Fatal(err)
			}
			output := filepath.Join(t.TempDir(), "scripts")
			if err = WriteCSVLoadPlan(dir, "gengo", output); err == nil {
				t.Fatal("invalid input accepted")
			}
			if _, err = os.Stat(output); !os.IsNotExist(err) {
				t.Fatalf("failed plan was published: %v", err)
			}
		})
	}
}

func TestCSVLoadPlanRejectsInvalidSchema(t *testing.T) {
	for _, name := range []string{"", "a.b", `bad"name`, strings.Repeat("a", 64), "bad\nname"} {
		if err := WriteCSVLoadPlan("unused", name, filepath.Join(t.TempDir(), "scripts")); err == nil {
			t.Fatalf("accepted schema %q", name)
		}
	}
}
