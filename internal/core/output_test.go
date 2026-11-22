package core

import (
	"os"
	"path/filepath"
	"testing"

	financial "github.com/peekknuf/Gengo/internal/simulation/financial"
)

func TestPublishCompletedDataset(t *testing.T) {
	cfg := fixedConfig(t, 0, 2, 100000)
	parent := t.TempDir()
	dir := filepath.Join(parent, "run")
	if err := os.Mkdir(dir, 0755); err != nil {
		t.Fatal(err)
	}
	invalid := financial.FinancialRowCounts{Companies: 1, DailyStockPrices: 1}
	if err := GenerateModelData("financial", invalid, "csv", dir, cfg); err == nil {
		t.Fatal("invalid plan succeeded")
	}
	if err := GenerateModelData("financial", financial.FinancialRowCounts{Companies: 1, Exchanges: 1, DailyStockPrices: 1}, "invalid-format", dir, cfg); err == nil {
		t.Fatal("invalid format reported success")
	}
	if err := GenerateRun(RunPlan{Model: "financial", Rows: financial.FinancialRowCounts{Companies: 1, Exchanges: 1, DailyStockPrices: 10000000}, Format: "csv", Config: fixedConfig(t, 0, 2, 4)}, dir); err == nil {
		t.Fatal("failed price generation reported success")
	}
	entries, err := os.ReadDir(parent)
	if err != nil || len(entries) != 1 {
		t.Fatalf("after failure: %v %v", entries, err)
	}
	entries, err = os.ReadDir(dir)
	if err != nil || len(entries) != 0 {
		t.Fatalf("incomplete run published: %v %v", entries, err)
	}
	plan := financial.FinancialRowCounts{Companies: 2, Exchanges: 1, DailyStockPrices: 5}
	if err := GenerateModelData("financial", plan, "csv", dir, cfg); err != nil {
		t.Fatal(err)
	}
	if err := GenerateModelData("financial", plan, "csv", dir, cfg); err == nil {
		t.Fatal("nonempty destination accepted")
	}
	entries, err = os.ReadDir(dir)
	if err != nil || len(entries) != 4 {
		t.Fatalf("published files: %v %v", entries, err)
	}
}
