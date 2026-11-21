package ecommerceds

import (
	"os"
	"path/filepath"
	"testing"
)

func TestShardCreationFailure(t *testing.T) {
	ids := []int64{1}
	dims := SaleDimensions{Dates: ids, Times: ids, Customers: []CustomerKeys{{ID: 1, Demographics: 1, Household: 1, Address: 1}}}
	for _, format := range []string{"csv", "parquet"} {
		dir := t.TempDir()
		if err := os.Mkdir(filepath.Join(dir, "fact_store_sales_0."+format), 0755); err != nil {
			t.Fatal(err)
		}
		if err := GenerateStoreSalesOptimized(3, dims, ids, ids, ids, dir, format); err == nil {
			t.Fatalf("%s swallowed shard error", format)
		}
	}
}
