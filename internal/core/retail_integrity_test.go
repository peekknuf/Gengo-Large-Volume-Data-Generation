package core

import (
	"encoding/csv"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

func TestRetailSaleKeys(t *testing.T) {
	old := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(old)
	plan := ECommerceDSRowCounts{Customers: 1, CustomerAddresses: 1, CustomerDemographics: 1, HouseholdDemographics: 1, Items: 1, Promotions: 1, Stores: 1, CallCenters: 1, CatalogPages: 1, WebSites: 1, WebPages: 1, Warehouses: 1, Reasons: 1, ShipModes: 1, IncomeBands: 1, StoreSales: 4, CatalogSales: 4, WebSales: 4}
	for _, format := range []string{"csv", "parquet"} {
		t.Run(format, func(t *testing.T) {
			dir := t.TempDir()
			if err := generateECommerceDSDataConcurrently(plan, format, dir); err != nil {
				t.Fatal(err)
			}
			for _, tc := range []struct {
				table            string
				customer, groups int
			}{{"store", 3, 1}, {"catalog", 3, 2}, {"web", 4, 2}} {
				path := filepath.Join(dir, "fact_"+tc.table+"_sales_0."+format)
				rows := 0
				readRetailKeys(t, path, format, tc.customer+4*tc.groups, func(row []int64) {
					rows++
					if row[0] < 18262 || row[0] > 20453 || row[1] < 0 || row[1] >= 86400 {
						t.Fatalf("%s row %d invalid sale date/time %v", tc.table, rows, row[:2])
					}
					if tc.groups == 2 && (row[2] < row[0] || row[2] > 20453) {
						t.Fatalf("%s invalid ship date %d for sale %d", tc.table, row[2], row[0])
					}
					for _, v := range row[tc.customer:] {
						if v != 1 {
							t.Fatalf("%s invalid customer reference %d", tc.table, v)
						}
					}
				})
				if rows != 4 {
					t.Fatalf("%s rows = %d, want 4", tc.table, rows)
				}
			}
		})
	}
}

func readRetailKeys(t *testing.T, path, format string, columns int, check func([]int64)) {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	values := make([]int64, columns)
	if format == "csv" {
		r := csv.NewReader(f)
		if _, err := r.Read(); err != nil {
			t.Fatal(err)
		}
		for {
			row, err := r.Read()
			if err == io.EOF {
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			for i := range values {
				v, err := strconv.ParseInt(row[i], 10, 64)
				if err != nil {
					t.Fatal(err)
				}
				values[i] = v
			}
			check(values)
		}
	}
	tbl, err := pqarrow.ReadTable(t.Context(), f, nil, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
	if err != nil {
		t.Fatal(err)
	}
	defer tbl.Release()
	reader := array.NewTableReader(tbl, 64)
	defer reader.Release()
	for reader.Next() {
		rec := reader.Record()
		for r := 0; r < int(rec.NumRows()); r++ {
			for i := range values {
				values[i] = rec.Column(i).(*array.Int64).Value(r)
			}
			check(values)
		}
	}
	if err := reader.Err(); err != nil {
		t.Fatal(err)
	}
}
