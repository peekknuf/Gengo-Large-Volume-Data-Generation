package core

import (
	financial "github.com/peekknuf/Gengo/internal/simulation/financial"
	"os"
	"path/filepath"
	"testing"
)

func TestPublishCompletedDataset(t *testing.T) {
	parent := t.TempDir()
	dir := filepath.Join(parent, "run")
	if err := os.Mkdir(dir, 0755); err != nil {
		t.Fatal(err)
	}
	invalid := financial.FinancialRowCounts{Companies: 1, DailyStockPrices: 1}
	if err := GenerateModelData("financial", invalid, "csv", dir); err == nil {
		t.Fatal("invalid plan succeeded")
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
	if err := GenerateModelData("financial", plan, "csv", dir); err != nil {
		t.Fatal(err)
	}
	if err := GenerateModelData("financial", plan, "csv", dir); err == nil {
		t.Fatal("nonempty destination accepted")
	}
	entries, err = os.ReadDir(dir)
	if err != nil || len(entries) != 3 {
		t.Fatalf("published files: %v %v", entries, err)
	}
}
