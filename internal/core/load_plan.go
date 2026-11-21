package core

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strings"

	"github.com/peekknuf/Gengo/internal/schema"
)

var postgresSchemaName = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,62}$`)

// WriteCSVLoadPlan prepares client-side psql import scripts for a completed run.
// All source validation finishes before the output directory is published.
func WriteCSVLoadPlan(datasetDir, schemaName, outputDir string) error {
	if !postgresSchemaName.MatchString(schemaName) {
		return fmt.Errorf("schema must be an ASCII PostgreSQL identifier of 1 to 63 characters")
	}
	datasetDir, err := filepath.Abs(datasetDir)
	if err != nil {
		return err
	}
	if strings.ContainsAny(datasetDir, "\r\n\x00") {
		return fmt.Errorf("dataset path contains characters unsupported by psql import scripts")
	}
	tables, err := schema.ECommerce()
	if err != nil {
		return err
	}
	manifest, files, err := readLoadManifest(datasetDir, tables)
	if err != nil {
		return err
	}
	scripts := renderLoadScripts(datasetDir, schemaName, tables, manifest, files)
	destination, err := filepath.Abs(outputDir)
	if err != nil {
		return err
	}
	if err = checkEmptyDestination(destination); err != nil {
		return err
	}
	if err = os.MkdirAll(filepath.Dir(destination), 0755); err != nil {
		return err
	}
	staging, err := os.MkdirTemp(filepath.Dir(destination), "."+filepath.Base(destination)+"-partial-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(staging)
	for name, contents := range scripts {
		if err = os.WriteFile(filepath.Join(staging, name), []byte(contents), 0644); err != nil {
			return err
		}
	}
	if err = checkEmptyDestination(destination); err != nil {
		return err
	}
	if err = os.Remove(destination); err != nil && !os.IsNotExist(err) {
		return err
	}
	if err = os.Rename(staging, destination); err != nil {
		return fmt.Errorf("publish import scripts: %w", err)
	}
	return nil
}

func readLoadManifest(dir string, tables []schema.Table) (Manifest, map[string][]OutputFile, error) {
	file, err := os.Open(filepath.Join(dir, "manifest.json"))
	if err != nil {
		return Manifest{}, nil, fmt.Errorf("read dataset manifest: %w", err)
	}
	defer file.Close()
	var manifest Manifest
	decoder := json.NewDecoder(file)
	if err = decoder.Decode(&manifest); err != nil {
		return manifest, nil, fmt.Errorf("decode dataset manifest: %w", err)
	}
	var extra any
	if err = decoder.Decode(&extra); err != io.EOF {
		return manifest, nil, fmt.Errorf("manifest must contain one JSON object")
	}
	if manifest.Version != 1 || manifest.Identity.SchemaVersion != SchemaVersion || !manifest.Complete || manifest.DatasetID == "" {
		return manifest, nil, fmt.Errorf("expected a completed version-1 dataset manifest")
	}
	if manifest.Identity.Model != "ecommerce" || manifest.Format != "csv" {
		return manifest, nil, fmt.Errorf("load planning currently supports ecommerce CSV datasets")
	}
	byTable := make(map[string]schema.Table, len(tables))
	for _, table := range tables {
		count, ok := manifest.Identity.Counts[table.Name]
		if !ok || count < 0 {
			return manifest, nil, fmt.Errorf("missing or invalid planned count for %s", table.Name)
		}
		byTable[table.Name] = table
	}
	if len(manifest.Identity.Counts) != len(tables) {
		return manifest, nil, fmt.Errorf("manifest table counts differ from the ecommerce schema")
	}
	files := make(map[string][]OutputFile, len(tables))
	counts := make(map[string]int64, len(tables))
	seen := make(map[string]bool, len(manifest.Files))
	for _, entry := range manifest.Files {
		table, ok := byTable[entry.Table]
		if !ok {
			return manifest, nil, fmt.Errorf("unknown manifest table %q", entry.Table)
		}
		namePattern := `^` + regexp.QuoteMeta(entry.Table) + `(_[0-9]+)*\.csv$`
		if !regexp.MustCompile(namePattern).MatchString(entry.Name) || seen[entry.Name] {
			return manifest, nil, fmt.Errorf("invalid or duplicate dataset filename %q", entry.Name)
		}
		seen[entry.Name] = true
		if entry.Rows < 0 || entry.Bytes < 0 || entry.Rows > math.MaxInt64-counts[entry.Table] {
			return manifest, nil, fmt.Errorf("invalid file counts for %s", entry.Name)
		}
		if err = checkLoadFile(filepath.Join(dir, entry.Name), entry, table); err != nil {
			return manifest, nil, err
		}
		counts[entry.Table] += entry.Rows
		files[entry.Table] = append(files[entry.Table], entry)
	}
	for _, table := range tables {
		if counts[table.Name] != manifest.Identity.Counts[table.Name] {
			return manifest, nil, fmt.Errorf("manifest file rows differ from the planned count for %s", table.Name)
		}
		sort.Slice(files[table.Name], func(i, j int) bool {
			a, b := files[table.Name][i], files[table.Name][j]
			if a.FirstKey == b.FirstKey {
				return a.Name < b.Name
			}
			return a.FirstKey < b.FirstKey
		})
	}
	return manifest, files, nil
}

func checkLoadFile(path string, entry OutputFile, table schema.Table) error {
	info, err := os.Lstat(path)
	if err != nil {
		return fmt.Errorf("inspect %s: %w", entry.Name, err)
	}
	if !info.Mode().IsRegular() || info.Size() != entry.Bytes {
		return fmt.Errorf("%s must be a regular file matching its manifest byte size", entry.Name)
	}
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	header, err := csv.NewReader(file).Read()
	if err != nil {
		return fmt.Errorf("read CSV header from %s: %w", entry.Name, err)
	}
	expected := make([]string, len(table.Columns))
	for i, column := range table.Columns {
		expected[i] = column.Name
	}
	if !slices.Equal(header, expected) {
		return fmt.Errorf("CSV header for %s differs from the ecommerce schema", entry.Name)
	}
	return nil
}

func quotePostgresIdentifier(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

func renderLoadScripts(dir, namespace string, tables []schema.Table, manifest Manifest, files map[string][]OutputFile) map[string]string {
	qualified := func(table string) string {
		return quotePostgresIdentifier(namespace) + "." + quotePostgresIdentifier(table)
	}
	var ddl, load, indexes, foreign, validate, verify, analyze strings.Builder
	ddl.WriteString("BEGIN;\nCREATE SCHEMA " + quotePostgresIdentifier(namespace) + ";\n\n")
	load.WriteString("BEGIN;\n")
	indexes.WriteString("BEGIN;\n")
	foreign.WriteString("BEGIN;\n")
	validate.WriteString("BEGIN;\n")
	verify.WriteString("DO $gengo_counts$\nDECLARE actual bigint;\nBEGIN\n")
	for _, table := range tables {
		name := qualified(table.Name)
		columnNames := make([]string, len(table.Columns))
		var textColumns []string
		fmt.Fprintf(&ddl, "CREATE TABLE %s (\n", name)
		for i, column := range table.Columns {
			columnNames[i] = quotePostgresIdentifier(column.Name)
			if i > 0 {
				ddl.WriteString(",\n")
			}
			fmt.Fprintf(&ddl, "  %s %s NOT NULL", columnNames[i], column.PostgresType)
			if column.PostgresType == "text" {
				textColumns = append(textColumns, columnNames[i])
			}
		}
		ddl.WriteString("\n);\n\n")
		options := "FORMAT csv, HEADER true, ENCODING 'UTF8'"
		if len(textColumns) > 0 {
			options += ", FORCE_NOT_NULL (" + strings.Join(textColumns, ", ") + ")"
		}
		for _, entry := range files[table.Name] {
			path := strings.ReplaceAll(filepath.ToSlash(filepath.Join(dir, entry.Name)), "'", "''")
			fmt.Fprintf(&load, "\\copy %s (%s) FROM '%s' WITH (%s)\n", name, strings.Join(columnNames, ", "), path, options)
		}
		fmt.Fprintf(&indexes, "ALTER TABLE %s ADD CONSTRAINT %s PRIMARY KEY (%s);\n", name, quotePostgresIdentifier(table.Name+"_pkey"), quotePostgresIdentifier(table.PrimaryKey))
		for _, key := range table.ForeignKeys {
			constraint := quotePostgresIdentifier(table.Name + "_" + key.Column + "_fkey")
			fmt.Fprintf(&foreign, "ALTER TABLE %s ADD CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s (%s) NOT VALID;\n", name, constraint, quotePostgresIdentifier(key.Column), qualified(key.ParentTable), quotePostgresIdentifier(key.ParentColumn))
			fmt.Fprintf(&validate, "ALTER TABLE %s VALIDATE CONSTRAINT %s;\n", name, constraint)
		}
		fmt.Fprintf(&verify, "  SELECT count(*) INTO actual FROM %s;\n  IF actual <> %d THEN\n    RAISE EXCEPTION 'Row count mismatch for %s. Expected %d, got %%', actual;\n  END IF;\n", name, manifest.Identity.Counts[table.Name], table.Name, manifest.Identity.Counts[table.Name])
		fmt.Fprintf(&analyze, "ANALYZE %s;\n", name)
	}
	ddl.WriteString("COMMIT;\n")
	load.WriteString("\\ir verify.sql\nCOMMIT;\n")
	indexes.WriteString("COMMIT;\n")
	foreign.WriteString("COMMIT;\n")
	validate.WriteString("COMMIT;\n")
	verify.WriteString("END;\n$gengo_counts$;\n")
	scripts := map[string]string{"schema.sql": ddl.String(), "load.sql": load.String(), "indexes.sql": indexes.String(), "foreign_keys.sql": foreign.String(), "validate_foreign_keys.sql": validate.String(), "verify.sql": verify.String(), "analyze.sql": analyze.String()}
	var run strings.Builder
	run.WriteString("\\set ON_ERROR_STOP on\n\\encoding UTF8\n\\timing on\nSET standard_conforming_strings = on;\n")
	for _, stage := range []struct{ label, file string }{{"Create schema and tables", "schema.sql"}, {"Copy CSV data and verify counts", "load.sql"}, {"Build primary-key indexes", "indexes.sql"}, {"Add foreign keys", "foreign_keys.sql"}, {"Validate foreign keys", "validate_foreign_keys.sql"}, {"Collect statistics", "analyze.sql"}} {
		fmt.Fprintf(&run, "\\echo %s\n\\ir %s\n", stage.label, stage.file)
	}
	scripts["run.psql"] = run.String()
	return scripts
}
