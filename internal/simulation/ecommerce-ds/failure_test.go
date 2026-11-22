package ecommerceds

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/peekknuf/Gengo/internal/generation"
)

func TestShardCreationFailure(t *testing.T) {
	seed := uint64(0)
	cfg, err := generation.Resolve(&seed, "2026-11-21", 4, 2)
	if err != nil {
		t.Fatal(err)
	}
	ids := []int64{1}
	dims := SaleDimensions{Dates: ids, Times: ids, Customers: []CustomerKeys{{ID: 1, Demographics: 1, Household: 1, Address: 1}}}
	for _, format := range []string{"csv", "parquet"} {
		dir := t.TempDir()
		if err := os.Mkdir(filepath.Join(dir, "fact_store_sales_0."+format), 0755); err != nil {
			t.Fatal(err)
		}
		if err := GenerateStoreSalesOptimized(t.Context(), 3, dims, ids, ids, ids, dir, format, cfg); err == nil {
			t.Fatalf("%s swallowed shard error", format)
		}
	}
}
