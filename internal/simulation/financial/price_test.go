package financial

import (
	"encoding/csv"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/peekknuf/Gengo/internal/generation"
	"github.com/peekknuf/Gengo/internal/models/financial"
)

func TestPriceDistribution(t *testing.T) {
	seed := uint64(0)
	cfg, err := generation.Resolve(&seed, "2026-11-21", 100000, 2)
	if err != nil {
		t.Fatal(err)
	}
	companies := []financial.Company{{CompanyID: 1}, {CompanyID: 2}, {CompanyID: 3}}
	exchanges := []financial.Exchange{{ExchangeID: 1}}
	for _, count := range []int{0, 2, 3, 17} {
		dir := t.TempDir()
		err := generateAndWriteDailyStockPricesConcurrently(count, companies, exchanges, "csv", dir, cfg)
		if err != nil {
			t.Fatal(err)
		}
		if count == 0 {
			continue
		}

		files, err := filepath.Glob(filepath.Join(dir, "fact_daily_stock_prices_*.csv"))
		if err != nil {
			t.Fatal(err)
		}
		rows := [][]string{{}}
		for _, path := range files {
			f, err := os.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			shard, err := csv.NewReader(f).ReadAll()
			f.Close()
			if err != nil {
				t.Fatal(err)
			}
			rows = append(rows, shard[1:]...)
		}

		if len(rows) != count+1 {
			t.Fatalf("requested %d prices, got %d", count, len(rows)-1)
		}
		seen := map[int]bool{}
		for _, r := range rows[1:] {
			id, err := strconv.Atoi(r[0])
			if err != nil || id < 1 || id > count || seen[id] {
				t.Fatalf("invalid price ID %s", r[0])
			}
			seen[id] = true
		}
	}
	if err := generateAndWriteDailyStockPricesConcurrently(1, nil, exchanges, "csv", t.TempDir(), cfg); err == nil {
		t.Fatal("accepted prices without companies")
	}
	for _, p := range generateStockPricesForCompanies(companies, exchanges, 10000, 1, 1, cfg) {
		if !(p.LowPrice > 0 && p.LowPrice <= p.OpenPrice && p.OpenPrice <= p.HighPrice && p.LowPrice <= p.ClosePrice && p.ClosePrice <= p.HighPrice) || math.IsInf(p.HighPrice, 0) {
			t.Fatalf("invalid price %+v", p)
		}
	}
}
