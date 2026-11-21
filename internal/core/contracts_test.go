package core

import (
	"encoding/csv"
	"io"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"strconv"

	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	financial "github.com/peekknuf/Gengo/internal/simulation/financial"
	medical "github.com/peekknuf/Gengo/internal/simulation/medical"
)

// Tables are in dependency order. Each foreign key names a previously checked table.
type tableContract struct {
	name, primary string
	count         int
	foreign       map[string]string
}

func checkTable(t *testing.T, dir, format string, spec tableContract, keys map[string]map[string]bool) {
	t.Helper()
	seen := map[string]bool{}
	keys[spec.name] = seen
	files, err := filepath.Glob(filepath.Join(dir, spec.name+"_*."+format))
	if err != nil {
		t.Fatal(err)
	}
	single := filepath.Join(dir, spec.name+"."+format)
	if _, err := os.Stat(single); err == nil {
		files = append(files, single)
	}
	for _, path := range files {
		readKeyColumns(t, path, format, spec, func(id string, refs map[string]string) {
			if id == "" || seen[id] {
				t.Fatalf("%s duplicate or empty %s=%q", path, spec.primary, id)
			}
			seen[id] = true
			for field, v := range refs {
				if !keys[spec.foreign[field]][v] {
					t.Fatalf("%s %s=%s: missing parent for %s=%s", path, spec.primary, id, field, v)
				}
			}
		})
	}
	if len(seen) != spec.count {
		t.Fatalf("%s got %d rows, want %d", spec.name, len(seen), spec.count)
	}
}

func readKeyColumns(t *testing.T, path, format string, spec tableContract, visit func(string, map[string]string)) {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	columns := []string{spec.primary}
	for field := range spec.foreign {
		columns = append(columns, field)
	}
	indexes := make([]int, len(columns))
	locate := func(names []string) {
		for i, name := range columns {
			indexes[i] = -1
			for j, v := range names {
				if v == name {
					indexes[i] = j
					break
				}
			}
			if indexes[i] < 0 {
				t.Fatalf("%s missing %s", path, name)
			}
		}
	}
	refs := map[string]string{}
	check := func(value func(int) string) {
		for i, key := range columns[1:] {
			refs[key] = value(indexes[i+1])
		}
		visit(value(indexes[0]), refs)
	}
	if format == "csv" {
		r := csv.NewReader(f)
		r.ReuseRecord = true
		names, err := r.Read()
		if err != nil {
			t.Fatal(err)
		}
		locate(names)
		for {
			row, err := r.Read()
			if err == io.EOF {
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			check(func(i int) string { return row[i] })
		}
	}
	tbl, err := pqarrow.ReadTable(t.Context(), f, nil, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
	if err != nil {
		t.Fatal(err)
	}
	defer tbl.Release()
	var names []string
	for _, field := range tbl.Schema().Fields() {
		names = append(names, field.Name)
	}
	locate(names)
	r := array.NewTableReader(tbl, 1024)
	defer r.Release()
	for r.Next() {
		rec := r.Record()
		for j := 0; j < int(rec.NumRows()); j++ {
			check(func(i int) string { return rec.Column(i).ValueStr(j) })
		}
	}
	if err := r.Err(); err != nil {
		t.Fatal(err)
	}
}

func checkRetailAmounts(t *testing.T, dir, format string) {
	t.Helper()
	for _, channel := range []struct{ name, prefix string }{{"store", "ss_"}, {"catalog", "cs_"}, {"web", "ws_"}} {
		fields := map[string]string{}
		for _, suffix := range []string{"ext_sales_price", "coupon_amt", "net_paid", "ext_list_price", "ext_discount_amt"} {
			fields[channel.prefix+suffix] = ""
		}
		spec := tableContract{name: "fact_" + channel.name + "_sales", primary: channel.prefix + "item_sk", foreign: fields}
		files, err := filepath.Glob(filepath.Join(dir, spec.name+"_*."+format))
		if err != nil {
			t.Fatal(err)
		}
		for _, path := range files {
			readKeyColumns(t, path, format, spec, func(_ string, refs map[string]string) {
				cents := func(suffix string) int64 {
					v, err := strconv.ParseFloat(refs[channel.prefix+suffix], 64)
					if err != nil {
						t.Fatal(err)
					}
					return int64(math.Round(100 * v))
				}
				if cents("net_paid") != cents("ext_sales_price")-cents("coupon_amt") || cents("ext_sales_price") != cents("ext_list_price")-cents("ext_discount_amt") {
					t.Fatalf("%s inconsistent sale amounts %v", path, refs)
				}
			})
		}
	}
}
func TestSmallDatasetContracts(t *testing.T) {
	old := runtime.GOMAXPROCS(3)
	defer runtime.GOMAXPROCS(old)
	for _, format := range []string{"csv", "parquet"} {
		cases := []struct {
			model  string
			plan   any
			tables []tableContract
		}{
			{"ecommerce-ds", ECommerceDSRowCounts{Customers: 2, CustomerAddresses: 2, CustomerDemographics: 2, HouseholdDemographics: 2, Items: 2, Promotions: 2, Stores: 2, CallCenters: 2, CatalogPages: 2, WebSites: 2, WebPages: 2, Warehouses: 2, Reasons: 2, ShipModes: 2, IncomeBands: 2, StoreSales: 7, CatalogSales: 7, WebSales: 7}, []tableContract{
				{"dim_date", "d_date_sk", 2192, nil},
				{"dim_time", "t_time_sk", 86400, nil},
				{"dim_income_bands", "ib_income_band_sk", 2, nil},
				{"dim_household_demographics", "hd_demo_sk", 2, map[string]string{"hd_income_band_sk": "dim_income_bands"}},
				{"dim_customer_demographics", "cd_demo_sk", 2, nil},
				{"dim_customer_addresses", "ca_address_sk", 2, map[string]string{"ca_address_date_sk": "dim_date"}},
				{"dim_customers", "c_customer_sk", 2, map[string]string{"c_current_cdemo_sk": "dim_customer_demographics", "c_current_hdemo_sk": "dim_household_demographics", "c_current_addr_sk": "dim_customer_addresses", "c_first_shipto_date_sk": "dim_date", "c_first_sales_date_sk": "dim_date", "c_last_review_date_sk": "dim_date"}},
				{"dim_items", "i_item_sk", 2, nil},
				{"dim_promotions", "p_promo_sk", 2, map[string]string{"p_item_sk": "dim_items", "p_start_date_sk": "dim_date", "p_end_date_sk": "dim_date"}},
				{"dim_stores", "s_store_sk", 2, nil},
				{"dim_call_centers", "cc_call_center_sk", 2, map[string]string{"cc_open_date_sk": "dim_date"}},
				{"dim_catalog_pages", "cp_catalog_page_sk", 2, map[string]string{"cp_start_date_sk": "dim_date", "cp_end_date_sk": "dim_date"}},
				{"dim_web_sites", "web_site_sk", 2, map[string]string{"web_open_date_sk": "dim_date"}},
				{"dim_web_pages", "wp_web_page_sk", 2, map[string]string{"wp_creation_date_sk": "dim_date", "wp_access_date_sk": "dim_date", "wp_customer_sk": "dim_customers"}},
				{"dim_warehouses", "w_warehouse_sk", 2, nil},
				{"dim_ship_modes", "sm_ship_mode_sk", 2, nil},
				{"dim_reasons", "r_reason_sk", 2, nil},
				{"fact_store_sales", "ss_ticket_number", 7, map[string]string{"ss_sold_date_sk": "dim_date", "ss_sold_time_sk": "dim_time", "ss_item_sk": "dim_items", "ss_promo_sk": "dim_promotions", "ss_customer_sk": "dim_customers", "ss_cdemo_sk": "dim_customer_demographics", "ss_hdemo_sk": "dim_household_demographics", "ss_addr_sk": "dim_customer_addresses", "ss_store_sk": "dim_stores"}},
				{"fact_catalog_sales", "cs_order_number", 7, map[string]string{"cs_sold_date_sk": "dim_date", "cs_sold_time_sk": "dim_time", "cs_item_sk": "dim_items", "cs_promo_sk": "dim_promotions", "cs_bill_customer_sk": "dim_customers", "cs_bill_cdemo_sk": "dim_customer_demographics", "cs_bill_hdemo_sk": "dim_household_demographics", "cs_bill_addr_sk": "dim_customer_addresses", "cs_ship_customer_sk": "dim_customers", "cs_ship_cdemo_sk": "dim_customer_demographics", "cs_ship_hdemo_sk": "dim_household_demographics", "cs_ship_addr_sk": "dim_customer_addresses", "cs_ship_date_sk": "dim_date", "cs_ship_mode_sk": "dim_ship_modes", "cs_warehouse_sk": "dim_warehouses", "cs_call_center_sk": "dim_call_centers", "cs_catalog_page_sk": "dim_catalog_pages"}},
				{"fact_web_sales", "ws_order_number", 7, map[string]string{"ws_sold_date_sk": "dim_date", "ws_sold_time_sk": "dim_time", "ws_item_sk": "dim_items", "ws_promo_sk": "dim_promotions", "ws_bill_customer_sk": "dim_customers", "ws_bill_cdemo_sk": "dim_customer_demographics", "ws_bill_hdemo_sk": "dim_household_demographics", "ws_bill_addr_sk": "dim_customer_addresses", "ws_ship_customer_sk": "dim_customers", "ws_ship_cdemo_sk": "dim_customer_demographics", "ws_ship_hdemo_sk": "dim_household_demographics", "ws_ship_addr_sk": "dim_customer_addresses", "ws_ship_date_sk": "dim_date", "ws_ship_mode_sk": "dim_ship_modes", "ws_warehouse_sk": "dim_warehouses", "ws_web_site_sk": "dim_web_sites", "ws_web_page_sk": "dim_web_pages"}},
			}},
			{"medical", medical.MedicalRowCounts{Patients: 3, Doctors: 2, Clinics: 1, Appointments: 7}, []tableContract{
				{"dim_patients", "patient_id", 3, nil}, {"dim_doctors", "doctor_id", 2, nil}, {"dim_clinics", "clinic_id", 1, nil},
				{"fact_appointments", "appointment_id", 7, map[string]string{"patient_id": "dim_patients", "doctor_id": "dim_doctors", "clinic_id": "dim_clinics"}}}},
			{"financial", financial.FinancialRowCounts{Companies: 3, Exchanges: 2, DailyStockPrices: 7}, []tableContract{
				{"dim_companies", "company_id", 3, nil}, {"dim_exchanges", "exchange_id", 2, nil}, {"fact_daily_stock_prices", "price_id", 7, map[string]string{"company_id": "dim_companies", "exchange_id": "dim_exchanges"}}}},
			{"ecommerce", ECommerceRowCounts{Customers: 3, CustomerAddresses: 5, Suppliers: 2, ProductCategories: 10, Products: 3, OrderHeaders: 7, OrderItems: 39}, []tableContract{
				{"dim_customers", "customer_id", 3, nil}, {"dim_customer_addresses", "address_id", 5, map[string]string{"customer_id": "dim_customers"}}, {"dim_suppliers", "supplier_id", 2, nil}, {"dim_product_categories", "category_id", 10, nil},
				{"dim_products", "product_id", 3, map[string]string{"supplier_id": "dim_suppliers", "category_id": "dim_product_categories"}},
				{"fact_orders_header", "order_id", 7, map[string]string{"customer_id": "dim_customers", "shipping_address_id": "dim_customer_addresses", "billing_address_id": "dim_customer_addresses"}},
				{"fact_order_items", "order_item_id", 39, map[string]string{"order_id": "fact_orders_header", "product_id": "dim_products"}}}},
		}
		for _, tc := range cases {
			t.Run(tc.model+"_"+format, func(t *testing.T) {
				dir := t.TempDir()
				if err := GenerateModelData(tc.model, tc.plan, format, dir); err != nil {
					t.Fatal(err)
				}
				if tc.model == "ecommerce-ds" {
					checkRetailAmounts(t, dir, format)
				}
				keys := map[string]map[string]bool{}
				for _, table := range tc.tables {
					checkTable(t, dir, format, table, keys)
				}
			})
		}
	}
}
