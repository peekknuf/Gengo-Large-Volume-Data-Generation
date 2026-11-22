package core

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/peekknuf/Gengo/internal/generation"
	financial "github.com/peekknuf/Gengo/internal/simulation/financial"
	medical "github.com/peekknuf/Gengo/internal/simulation/medical"
)

func fixedConfig(t *testing.T, seed uint64, workers, rowsPerFile int) generation.Config {
	t.Helper()
	cfg, err := generation.Resolve(&seed, "2026-11-21", rowsPerFile, workers)
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

func readManifest(t *testing.T, dir string) Manifest {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(dir, "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	var m Manifest
	if err = json.Unmarshal(data, &m); err != nil {
		t.Fatal(err)
	}
	if !m.Complete || m.DatasetID == "" || m.Identity.Algorithm != generation.AlgorithmVersion {
		t.Fatalf("invalid manifest %+v", m)
	}
	counts := map[string]int64{}
	for _, f := range m.Files {
		info, err := os.Stat(filepath.Join(dir, f.Name))
		if err != nil {
			t.Fatal(err)
		}
		if info.Size() != f.Bytes {
			t.Fatalf("%s byte count mismatch", f.Name)
		}
		rows, first, last, err := inspectOutput(filepath.Join(dir, f.Name), m.Format, f.Table)
		if err != nil || rows != f.Rows || first != f.FirstKey || last != f.LastKey {
			t.Fatalf("%s manifest mismatch %v", f.Name, err)
		}
		counts[f.Table] += f.Rows
	}
	for table, want := range m.Identity.Counts {
		if counts[table] != want {
			t.Fatalf("%s manifest count mismatch", table)
		}
	}
	return m
}

func TestReproducibleRuns(t *testing.T) {
	retail := ECommerceDSRowCounts{Customers: 3, CustomerAddresses: 5, CustomerDemographics: 3, HouseholdDemographics: 2, Items: 4, Promotions: 2, Stores: 2, CallCenters: 2, CatalogPages: 2, WebSites: 2, WebPages: 3, Warehouses: 2, Reasons: 2, ShipModes: 2, IncomeBands: 2, StoreSales: 19, CatalogSales: 17, WebSales: 13}
	for _, tc := range []struct {
		model string
		rows  any
	}{
		{"medical", medical.MedicalRowCounts{Patients: 3, Doctors: 2, Clinics: 2, Appointments: 19}},
		{"financial", financial.FinancialRowCounts{Companies: 3, Exchanges: 2, DailyStockPrices: 19}},
		{"ecommerce", ECommerceRowCounts{Customers: 3, CustomerAddresses: 5, Suppliers: 2, Products: 4, ProductCategories: 10, OrderHeaders: 19, OrderItems: 103}},
		{"ecommerce-ds", retail},
	} {
		t.Run(tc.model, func(t *testing.T) {
			var csvDir string
			for _, format := range []string{"csv", "parquet"} {
				var baseline string
				var reference Manifest
				for _, workers := range []int{1, 2, 8, 1} {
					dir := t.TempDir()
					if err := GenerateRun(RunPlan{Model: tc.model, Rows: tc.rows, Format: format, Config: fixedConfig(t, 0, workers, 4)}, dir); err != nil {
						t.Fatal(err)
					}
					m := readManifest(t, dir)
					if tc.model == "ecommerce-ds" {
						checkRetailCustomerReferences(t, dir, m)
					}
					if baseline == "" {
						baseline = dir
						reference = m
						continue
					}
					if !reflect.DeepEqual(m.Files, reference.Files) || m.DatasetID != reference.DatasetID {
						t.Fatalf("workers=%d changed partitions or manifest identity", workers)
					}
					compareOutput(t, baseline, dir, m, format == "csv")
				}
				changed := t.TempDir()
				if err := GenerateRun(RunPlan{Model: tc.model, Rows: tc.rows, Format: format, Config: fixedConfig(t, 1, 2, 4)}, changed); err != nil {
					t.Fatal(err)
				}
				changedManifest := readManifest(t, changed)
				if tc.model == "ecommerce-ds" {
					checkRetailCustomerReferences(t, changed, changedManifest)
				}
				if changedManifest.DatasetID == reference.DatasetID {
					t.Fatal("seed did not change identity")
				}
				first := reference.Files[0].Name
				if reflect.DeepEqual(readLogicalFile(t, filepath.Join(baseline, first), format), readLogicalFile(t, filepath.Join(changed, first), format)) {
					t.Fatal("seed did not change data")
				}
				if format == "csv" {
					csvDir = baseline
				} else {
					csvManifest := readManifest(t, csvDir)
					if reference.DatasetID != csvManifest.DatasetID {
						t.Fatal("format changed logical identity")
					}
					for _, f := range reference.Files {
						csvName := strings.TrimSuffix(f.Name, ".parquet") + ".csv"
						assertLogicalFileEqual(t, filepath.Join(csvDir, csvName), filepath.Join(baseline, f.Name), "csv", "parquet")
					}
				}
			}
		})
	}
}

func compareOutput(t *testing.T, a, b string, m Manifest, byteIdentical bool) {
	t.Helper()
	for _, f := range m.Files {
		left, right := filepath.Join(a, f.Name), filepath.Join(b, f.Name)
		if byteIdentical {
			x, err := os.ReadFile(left)
			if err != nil {
				t.Fatal(err)
			}
			y, err := os.ReadFile(right)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(x, y) {
				t.Fatalf("%s differs across worker settings", f.Name)
			}
		} else {
			assertLogicalFileEqual(t, left, right, m.Format, m.Format)
		}
	}
}

type logicalFile struct {
	Columns []string
	Rows    [][]string
}

func assertLogicalFileEqual(t *testing.T, a, b, formatA, formatB string) {
	t.Helper()
	x, y := readLogicalFile(t, a, formatA), readLogicalFile(t, b, formatB)
	if !reflect.DeepEqual(x.Columns, y.Columns) {
		t.Fatalf("columns differ in %s and %s: %v vs %v", a, b, x.Columns, y.Columns)
	}
	if len(x.Rows) != len(y.Rows) {
		t.Fatalf("row count differs for %s", a)
	}
	for i := range x.Rows {
		if !reflect.DeepEqual(x.Rows[i], y.Rows[i]) {
			t.Fatalf("%s row %d differs\n%v\n%v", a, i, x.Rows[i], y.Rows[i])
		}
	}
}

func readLogicalFile(t *testing.T, path, format string) logicalFile {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var result logicalFile
	if format == "csv" {
		rows, err := csv.NewReader(f).ReadAll()
		if err != nil {
			t.Fatal(err)
		}
		result.Columns = rows[0]
		result.Rows = rows[1:]
		for j, name := range result.Columns {
			if name == "order_timestamp_unix" {
				result.Columns[j] = "order_timestamp"
				for _, row := range result.Rows {
					v, err := strconv.ParseInt(row[j], 10, 64)
					if err != nil {
						t.Fatal(err)
					}
					row[j] = time.Unix(v, 0).UTC().Format(time.RFC3339Nano)
				}
			}
		}
	} else {
		tbl, err := pqarrow.ReadTable(t.Context(), f, nil, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
		if err != nil {
			t.Fatal(err)
		}
		defer tbl.Release()
		for _, field := range tbl.Schema().Fields() {
			result.Columns = append(result.Columns, field.Name)
		}
		r := array.NewTableReader(tbl, 1024)
		defer r.Release()
		for r.Next() {
			record := r.Record()
			for i := 0; i < int(record.NumRows()); i++ {
				row := make([]string, len(result.Columns))
				for j := range row {
					column := record.Column(j)
					switch c := column.(type) {
					case *array.Timestamp:
						unit := c.DataType().(*arrow.TimestampType).Unit
						row[j] = time.Unix(0, int64(c.Value(i))*int64(unit.Multiplier())).UTC().Format(time.RFC3339Nano)
					default:
						row[j] = column.ValueStr(i)
					}
				}
				result.Rows = append(result.Rows, row)
			}
		}
		if err = r.Err(); err != nil {
			t.Fatal(err)
		}
	}
	for _, row := range result.Rows {
		for j, v := range row {
			if date, err := time.Parse(time.RFC3339Nano, v); err == nil {
				row[j] = date.UTC().Format(time.RFC3339Nano)
				continue
			}
			if strings.Contains(v, ".") {
				if number, err := strconv.ParseFloat(v, 64); err == nil {
					row[j] = fmt.Sprint(number)
				}
			}
		}
	}
	return result
}

func primaryColumn(table string) int {
	switch table {
	case "fact_store_sales":
		return 9
	case "fact_catalog_sales", "fact_web_sales":
		return 17
	}
	return 0
}

func inspectOutput(path, format, table string) (rows, first, last int64, err error) {
	column := primaryColumn(table)
	accept := func(value string) error {
		key, e := strconv.ParseInt(value, 10, 64)
		if e != nil {
			return e
		}
		if rows == 0 {
			first = key
		}
		last = key
		rows++
		return nil
	}
	if format == "csv" {
		f, e := os.Open(path)
		if e != nil {
			return 0, 0, 0, e
		}
		defer f.Close()
		r := csv.NewReader(f)
		r.ReuseRecord = true
		if _, e = r.Read(); e == io.EOF {
			return 0, 0, 0, nil
		} else if e != nil {
			return 0, 0, 0, e
		}
		for {
			record, e := r.Read()
			if e == io.EOF {
				break
			}
			if e != nil {
				return 0, 0, 0, e
			}
			if column >= len(record) {
				return 0, 0, 0, fmt.Errorf("missing primary key column")
			}
			if e = accept(record[column]); e != nil {
				return 0, 0, 0, e
			}
		}
		return rows, first, last, nil
	}
	f, e := file.OpenParquetFile(path, false)
	if e != nil {
		return 0, 0, 0, e
	}
	defer f.Close()
	reader, e := pqarrow.NewFileReader(f, pqarrow.ArrowReadProperties{BatchSize: 65536}, memory.DefaultAllocator)
	if e != nil {
		return 0, 0, 0, e
	}
	records, e := reader.GetRecordReader(context.Background(), []int{column}, nil)
	if e != nil {
		return 0, 0, 0, e
	}
	defer records.Release()
	for records.Next() {
		record := records.Record()
		for i := 0; i < int(record.NumRows()); i++ {
			if e = accept(record.Column(0).ValueStr(i)); e != nil {
				return 0, 0, 0, e
			}
		}
	}
	if e = records.Err(); e != nil && e != io.EOF {
		return 0, 0, 0, e
	}
	if rows != f.NumRows() {
		return 0, 0, 0, fmt.Errorf("key column row count differs from Parquet footer")
	}
	return rows, first, last, nil
}

func checkRetailCustomerReferences(t *testing.T, dir string, m Manifest) {
	t.Helper()
	customers := readLogicalFile(t, filepath.Join(dir, "dim_customers."+m.Format), m.Format)
	fields := map[string]int{}
	for i, name := range customers.Columns {
		fields[name] = i
	}
	byID := map[string][3]string{}
	for _, row := range customers.Rows {
		byID[row[fields["c_customer_sk"]]] = [3]string{row[fields["c_current_cdemo_sk"]], row[fields["c_current_hdemo_sk"]], row[fields["c_current_addr_sk"]]}
	}
	for _, f := range m.Files {
		if !strings.HasPrefix(f.Table, "fact_") {
			continue
		}
		rows := readLogicalFile(t, filepath.Join(dir, f.Name), m.Format)
		indexes := map[string]int{}
		for i, name := range rows.Columns {
			indexes[name] = i
		}
		var prefixes []string
		switch f.Table {
		case "fact_store_sales":
			prefixes = []string{"ss_"}
		case "fact_catalog_sales":
			prefixes = []string{"cs_bill_", "cs_ship_"}
		case "fact_web_sales":
			prefixes = []string{"ws_bill_", "ws_ship_"}
		}
		for _, prefix := range prefixes {
			for _, row := range rows.Rows {
				id := row[indexes[prefix+"customer_sk"]]
				want, ok := byID[id]
				got := [3]string{row[indexes[prefix+"cdemo_sk"]], row[indexes[prefix+"hdemo_sk"]], row[indexes[prefix+"addr_sk"]]}
				if !ok || got != want {
					t.Fatalf("%s customer %s references %v, want %v", f.Name, id, got, want)
				}
			}
		}
	}
}

func TestFinancialFileBoundariesPreserveSeries(t *testing.T) {
	for _, format := range []string{"csv", "parquet"} {
		var reference [][]string
		for _, rowsPerFile := range []int{4, 100} {
			dir := t.TempDir()
			plan := RunPlan{Model: "financial", Rows: financial.FinancialRowCounts{Companies: 2, Exchanges: 2, DailyStockPrices: 31}, Format: format, Config: fixedConfig(t, 0, 2, rowsPerFile)}
			if err := GenerateRun(plan, dir); err != nil {
				t.Fatal(err)
			}
			manifest := readManifest(t, dir)
			var rows [][]string
			for _, f := range manifest.Files {
				if f.Table == "fact_daily_stock_prices" {
					rows = append(rows, readLogicalFile(t, filepath.Join(dir, f.Name), format).Rows...)
				}
			}
			sort.Slice(rows, func(i, j int) bool { a, _ := strconv.Atoi(rows[i][0]); b, _ := strconv.Atoi(rows[j][0]); return a < b })
			if reference == nil {
				reference = rows
			} else if !reflect.DeepEqual(reference, rows) {
				t.Fatal("file boundary changed the financial series")
			}
		}
	}
}

func TestZeroCountRuns(t *testing.T) {
	for _, tc := range []struct {
		model string
		rows  any
	}{{"ecommerce", ECommerceRowCounts{}}, {"ecommerce-ds", ECommerceDSRowCounts{}}, {"financial", financial.FinancialRowCounts{}}, {"medical", medical.MedicalRowCounts{}}} {
		for _, format := range []string{"csv", "parquet"} {
			dir := t.TempDir()
			if err := GenerateRun(RunPlan{Model: tc.model, Rows: tc.rows, Format: format, Config: fixedConfig(t, 0, 2, 4)}, dir); err != nil {
				t.Fatalf("%s %s: %v", tc.model, format, err)
			}
			readManifest(t, dir)
		}
	}
}
