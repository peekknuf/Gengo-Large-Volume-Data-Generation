package benchmark

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func customScenario(t *testing.T, sql string, dataset bool) string {
	t.Helper()
	contents := strings.ReplaceAll(validScenario, "workload: customer-lookup", "workload: custom-sql\nsql_file: query.sql")
	if !dataset {
		contents = strings.ReplaceAll(contents, "dataset: dataset\n", "")
		contents = strings.ReplaceAll(contents, "schema: gengo_test\n", "")
	}
	path := scenarioFixture(t, contents)
	if err := os.WriteFile(filepath.Join(filepath.Dir(path), "query.sql"), []byte(sql), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestCustomSQLLoadAndSnapshot(t *testing.T) {
	for _, dataset := range []bool{false, true} {
		path := customScenario(t, "\\set id random(1, 3)\nSELECT :id;", dataset)
		plan, err := LoadScenario(path)
		if err != nil {
			t.Fatal(err)
		}
		if plan.SQL != "BEGIN READ ONLY;\n\\set id random(1, 3)\nSELECT :id;\nCOMMIT;\n" || !filepath.IsAbs(plan.Scenario.SQLFile) || (plan.Dataset.DatasetID != "") != dataset {
			t.Fatalf("incorrect custom plan: %+v", plan)
		}
		if err := os.WriteFile(plan.Scenario.SQLFile, []byte("SELECT 999;"), 0600); err != nil {
			t.Fatal(err)
		}
		installClientHelpers(t)
		t.Setenv("GENGO_BENCH_HELPER", "custom")
		output := filepath.Join(t.TempDir(), "result")
		result, err := Run(t.Context(), plan, "", output, &bytes.Buffer{})
		if err == nil || result == nil || len(result.Steps) != 6 {
			t.Fatalf("expected full threshold-failing sweep: %+v, %v", result, err)
		}
		script, err := os.ReadFile(filepath.Join(output, "workload.sql"))
		if err != nil || string(script) != plan.SQL {
			t.Fatalf("script changed after planning: %q, %v", script, err)
		}
		hash := sha256.Sum256(script)
		if result.WorkloadSHA256 != hex.EncodeToString(hash[:]) {
			t.Fatal("saved workload identity differs from executed script")
		}
		loaded, err := ReadResult(filepath.Join(output, "result.json"))
		if err != nil {
			t.Fatal(err)
		}
		loaded.Scenario.SQLFile = "/a/different/path.sql"
		if _, err := Compare(result, loaded); err != nil {
			t.Fatalf("identical SQL at a different path rejected: %v", err)
		}
		loaded.WorkloadSHA256 = strings.Repeat("a", 64)
		if _, err := Compare(result, loaded); err == nil || !strings.Contains(err.Error(), "workload SQL identity") {
			t.Fatalf("changed SQL accepted: %v", err)
		}
		loaded.WorkloadSHA256 = ""
		if _, err := Compare(result, loaded); err == nil {
			t.Fatal("missing custom SQL identity accepted")
		}
	}
}

func TestCustomSQLInvalidFiles(t *testing.T) {
	for _, sql := range []string{"", " \n\t", "SELECT 1;\x00", strings.Repeat("x", (1<<20)+1)} {
		if _, err := LoadScenario(customScenario(t, sql, false)); err == nil {
			t.Fatal("accepted invalid SQL file")
		}
	}
	path := customScenario(t, "SELECT 1;", false)
	sqlPath := filepath.Join(filepath.Dir(path), "query.sql")
	if err := os.Remove(sqlPath); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadScenario(path); err == nil {
		t.Fatal("accepted missing SQL file")
	}
	if err := os.Mkdir(sqlPath, 0700); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadScenario(path); err == nil {
		t.Fatal("accepted SQL directory")
	}
	for _, contents := range []string{
		strings.ReplaceAll(validScenario, "workload: customer-lookup", "workload: custom-sql"),
		validScenario + "sql_file: query.sql\n",
	} {
		if _, err := LoadScenario(scenarioFixture(t, contents)); err == nil {
			t.Fatal("accepted invalid workload/file combination")
		}
	}
}
