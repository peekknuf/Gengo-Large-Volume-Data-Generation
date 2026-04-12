package tests

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"context"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

const defaultTestSizeGB = 0.1
const defaultTimeout = "10m"

func getTestSizeGB() float64 {
	if v := os.Getenv("GENGO_TEST_SIZE_GB"); v != "" {
		f, err := strconv.ParseFloat(v, 64)
		if err == nil && f > 0 {
			return f
		}
	}
	return defaultTestSizeGB
}

func getTestTimeout() time.Duration {
	if v := os.Getenv("GENGO_TEST_TIMEOUT"); v != "" {
		d, err := time.ParseDuration(v)
		if err == nil {
			return d
		}
	}
	d, _ := time.ParseDuration(defaultTimeout)
	return d
}

func keepOutput() bool {
	return os.Getenv("GENGO_KEEP_OUTPUT") == "1"
}

func moduleRoot(t *testing.T) string {
	t.Helper()
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot determine test file path")
	}
	return filepath.Join(filepath.Dir(filename), "..")
}

func binaryPath(t *testing.T) string {
	t.Helper()
	return filepath.Join(moduleRoot(t), "tests", "gengo-test")
}

func TestBuild(t *testing.T) {
	root := moduleRoot(t)
	bin := binaryPath(t)
	cmd := exec.Command("go", "build", "-o", bin, ".")
	cmd.Dir = root
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("failed to build binary: %v", err)
	}
	t.Logf("binary built at %s", bin)
}

type testCase struct {
	model  string
	format string
}

func (tc testCase) name() string {
	return tc.model + "_" + tc.format
}

func (tc testCase) outputDir(root string) string {
	return filepath.Join(root, "tests", "output_"+tc.model+"_"+tc.format)
}

func TestE2E(t *testing.T) {
	cases := []testCase{
		{"ecommerce", "csv"},
		{"ecommerce", "parquet"},
		{"ecommerce-ds", "csv"},
		{"ecommerce-ds", "parquet"},
		{"financial", "csv"},
		{"financial", "parquet"},
		{"medical", "csv"},
		{"medical", "parquet"},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name(), func(t *testing.T) {
			runE2ETest(t, tc)
		})
	}
}

func runE2ETest(t *testing.T, tc testCase) {
	t.Helper()
	root := moduleRoot(t)
	bin := binaryPath(t)
	size := getTestSizeGB()
	timeout := getTestTimeout()
	outputDir := tc.outputDir(root)

	if !keepOutput() {
		defer os.RemoveAll(outputDir)
	}

	start := time.Now()
	ctx := t.Context()
	cmd := exec.CommandContext(ctx, bin, "gen",
		"--model", tc.model,
		"--size", fmt.Sprintf("%.2f", size),
		"--format", tc.format,
		"--output", outputDir,
	)
	cmd.Dir = root
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	done := make(chan error, 1)
	go func() {
		done <- cmd.Run()
	}()

	var err error
	select {
	case err = <-done:
	case <-time.After(timeout):
		t.Fatalf("test timed out after %v", timeout)
	}
	if err != nil {
		t.Fatalf("CLI execution failed: %v", err)
	}
	elapsed := time.Since(start)

	totalBytes := getTotalDirSize(outputDir)
	throughputMBs := float64(totalBytes) / 1024.0 / 1024.0 / elapsed.Seconds()
	t.Logf("model=%s format=%s size=%.2fGB elapsed=%v bytes=%d throughput=%.1fMB/s",
		tc.model, tc.format, size, elapsed, totalBytes, throughputMBs)

	minThroughput := 30.0
	if tc.format == "csv" {
		minThroughput = 50.0
	}
	if throughputMBs < minThroughput {
		t.Logf("WARNING: throughput %.1fMB/s is below threshold %.0fMB/s", throughputMBs, minThroughput)
	}

	validateOutputDir(t, outputDir)
	validateSchemaFiles(t, outputDir, tc.model, tc.format)
}

func validateOutputDir(t *testing.T, dir string) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("cannot read output directory %s: %v", dir, err)
	}
	if len(entries) < 2 {
		t.Fatalf("expected at least 2 files in output dir, got %d", len(entries))
	}

	totalSize := int64(0)
	for _, e := range entries {
		if !e.IsDir() {
			info, err := e.Info()
			if err == nil {
				totalSize += info.Size()
			}
		}
	}
	if totalSize < 1024 {
		t.Fatalf("total output size %d bytes is suspiciously small (< 1KB)", totalSize)
	}
	t.Logf("output dir: %d files, %d bytes total", len(entries), totalSize)
}

type fileSpec struct {
	baseName string
	sharded  bool
}

func getSchemaSpecs(model string) (dims []string, facts []fileSpec) {
	switch model {
	case "ecommerce":
		dims = []string{
			"dim_customers",
			"dim_customer_addresses",
			"dim_suppliers",
			"dim_product_categories",
			"dim_products",
		}
		facts = []fileSpec{
			{baseName: "fact_orders_header", sharded: true},
			{baseName: "fact_order_items", sharded: true},
		}
	case "ecommerce-ds":
		dims = []string{
			"dim_items",
			"dim_customers",
			"dim_customer_addresses",
			"dim_customer_demographics",
			"dim_household_demographics",
			"dim_promotions",
			"dim_stores",
			"dim_call_centers",
			"dim_catalog_pages",
			"dim_web_sites",
			"dim_web_pages",
			"dim_warehouses",
			"dim_reasons",
			"dim_ship_modes",
			"dim_income_bands",
			"dim_time",
			"dim_date",
		}
		facts = []fileSpec{
			{baseName: "fact_store_sales", sharded: true},
			{baseName: "fact_catalog_sales", sharded: true},
			{baseName: "fact_web_sales", sharded: true},
		}
	case "financial":
		dims = []string{
			"dim_companies",
			"dim_exchanges",
		}
		facts = []fileSpec{
			{baseName: "fact_daily_stock_prices", sharded: false},
		}
	case "medical":
		dims = []string{
			"dim_patients",
			"dim_doctors",
			"dim_clinics",
		}
		facts = []fileSpec{
			{baseName: "fact_appointments", sharded: false},
		}
	}
	return
}

func getFileExt(format string) string {
	switch format {
	case "csv":
		return ".csv"
	case "parquet":
		return ".parquet"
	default:
		return "." + format
	}
}

func validateSchemaFiles(t *testing.T, dir, model, format string) {
	t.Helper()
	ext := getFileExt(format)
	dims, facts := getSchemaSpecs(model)

	for _, dim := range dims {
		path := filepath.Join(dir, dim+ext)
		if _, err := os.Stat(path); err != nil {
			t.Errorf("expected dimension file %s: %v", path, err)
			continue
		}
		validateFileContent(t, path, format, model, dim)
	}

	for _, fact := range facts {
		if fact.sharded {
			pattern := filepath.Join(dir, fact.baseName+"_*"+ext)
			matches, err := filepath.Glob(pattern)
			if err != nil {
				t.Errorf("glob error for %s: %v", pattern, err)
				continue
			}
			singleFile := filepath.Join(dir, fact.baseName+ext)
			if sf, err := os.Stat(singleFile); err == nil && !sf.IsDir() {
				matches = append(matches, singleFile)
			}
			if len(matches) == 0 {
				t.Errorf("expected at least one fact file matching %s or %s", pattern, singleFile)
				continue
			}
			for _, m := range matches {
				validateFileContent(t, m, format, model, fact.baseName)
			}
		} else {
			path := filepath.Join(dir, fact.baseName+ext)
			if _, err := os.Stat(path); err != nil {
				t.Errorf("expected fact file %s: %v", path, err)
				continue
			}
			validateFileContent(t, path, format, model, fact.baseName)
		}
	}
}

func validateFileContent(t *testing.T, path, format, model, tableName string) {
	t.Helper()
	switch format {
	case "csv":
		validateCSVContent(t, path, model, tableName)
	case "parquet":
		validateParquetContent(t, path, model, tableName)
	}
}

func validateCSVContent(t *testing.T, path, model, tableName string) {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Errorf("cannot open CSV file %s: %v", path, err)
		return
	}
	defer f.Close()

	reader := csv.NewReader(f)
	reader.FieldsPerRecord = -1

	header, err := reader.Read()
	if err != nil {
		t.Errorf("cannot read CSV header from %s: %v", path, err)
		return
	}
	if len(header) == 0 {
		t.Errorf("CSV header is empty in %s", path)
		return
	}

	expectedHeaders := getExpectedCSVHeaders(model, tableName)
	if expectedHeaders != nil {
		hasContent := false
		for _, h := range header {
			if strings.TrimSpace(h) != "" {
				hasContent = true
				break
			}
		}
		if hasContent {
			if len(header) != len(expectedHeaders) {
				t.Errorf("table %s: expected %d columns, got %d (header: %v)", tableName, len(expectedHeaders), len(header), header)
			} else {
				for i, h := range header {
					if strings.TrimSpace(h) != expectedHeaders[i] {
						t.Errorf("table %s: column %d expected %q, got %q", tableName, i, expectedHeaders[i], strings.TrimSpace(h))
					}
				}
			}
		}
		if !hasContent && len(header) != len(expectedHeaders) {
			t.Errorf("table %s: expected %d columns, got %d", tableName, len(expectedHeaders), len(header))
		}
	}

	for i := 0; i < 3; i++ {
		record, err := reader.Read()
		if err == io.EOF {
			t.Logf("table %s: only %d data rows (less than 3)", tableName, i)
			break
		}
		if err != nil {
			t.Errorf("error reading row %d from %s: %v", i+1, path, err)
			return
		}
		if len(record) != len(header) {
			t.Logf("table %s row %d: header has %d fields, got %d (possible unquoted commas in data)", tableName, i+1, len(header), len(record))
			continue
		}

		firstField := strings.TrimSpace(record[0])
		if _, err := strconv.Atoi(firstField); err != nil {
			t.Errorf("table %s row %d: first field %q is not a valid integer ID", tableName, i+1, firstField)
		}
	}
}

func validateParquetContent(t *testing.T, path, model, tableName string) {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Errorf("cannot open Parquet file %s: %v", path, err)
		return
	}
	defer f.Close()

	pxFile, err := pqarrow.ReadTable(context.Background(), f, nil, pqarrow.ArrowReadProperties{}, memory.NewGoAllocator())
	if err != nil {
		t.Errorf("cannot read Parquet file %s: %v", path, err)
		return
	}
	defer pxFile.Release()

	schema := pxFile.Schema()
	if schema.NumFields() == 0 {
		t.Errorf("Parquet file %s has no columns", path)
		return
	}

	expectedCols := getExpectedParquetColumns(model, tableName)
	if expectedCols != nil {
		if schema.NumFields() != len(expectedCols) {
			t.Errorf("table %s: expected %d columns, got %d", tableName, len(expectedCols), schema.NumFields())
		}
		for i, exp := range expectedCols {
			if i < schema.NumFields() && schema.Field(i).Name != exp {
				t.Errorf("table %s column %d: expected %q, got %q", tableName, i, exp, schema.Field(i).Name)
			}
		}
	}

	if pxFile.NumRows() == 0 {
		t.Errorf("Parquet file %s has zero rows", path)
		return
	}

	numCols := int(pxFile.NumCols())
	for colIdx := 0; colIdx < numCols && colIdx < 3; colIdx++ {
		col := pxFile.Column(colIdx)
		if col.Len() == 0 {
			continue
		}
		colName := schema.Field(colIdx).Name
		if col.NullN() == col.Len() {
			t.Errorf("table %s column %s: all values are null", tableName, colName)
		}
	}
}

func getExpectedCSVHeaders(model, tableName string) []string {
	switch model {
	case "ecommerce":
		return getECommerceCSVHeaders(tableName)
	case "ecommerce-ds":
		return getECommerceDSCSVHeaders(tableName)
	case "financial":
		return getFinancialCSVHeaders(tableName)
	case "medical":
		return getMedicalCSVHeaders(tableName)
	default:
		return nil
	}
}

func getECommerceCSVHeaders(tableName string) []string {
	switch tableName {
	case "dim_customers":
		return []string{"customer_id", "first_name", "last_name", "email"}
	case "dim_customer_addresses":
		return []string{"address_id", "customer_id", "address_type", "address", "city", "state", "zip", "country"}
	case "dim_suppliers":
		return []string{"supplier_id", "supplier_name", "country"}
	case "dim_product_categories":
		return []string{"category_id", "category_name"}
	case "dim_products":
		return []string{"product_id", "supplier_id", "product_name", "category_id", "base_price"}
	case "fact_orders_header":
		return []string{"order_id", "customer_id", "shipping_address_id", "billing_address_id", "order_timestamp_unix", "order_status"}
	case "fact_order_items":
		return []string{"order_item_id", "order_id", "product_id", "quantity", "unit_price", "discount"}
	default:
		return nil
	}
}

func getFinancialCSVHeaders(tableName string) []string {
	switch tableName {
	case "dim_companies":
		return []string{"company_id", "company_name", "ticker_symbol", "sector"}
	case "dim_exchanges":
		return []string{"exchange_id", "exchange_name", "country"}
	case "fact_daily_stock_prices":
		return []string{"price_id", "date", "company_id", "exchange_id", "open_price", "high_price", "low_price", "close_price", "volume"}
	default:
		return nil
	}
}

func getMedicalCSVHeaders(tableName string) []string {
	switch tableName {
	case "dim_patients":
		return []string{"patient_id", "patient_name", "date_of_birth", "gender"}
	case "dim_doctors":
		return []string{"doctor_id", "doctor_name", "specialization"}
	case "dim_clinics":
		return []string{"clinic_id", "clinic_name", "address"}
	case "fact_appointments":
		return []string{"appointment_id", "patient_id", "doctor_id", "clinic_id", "appointment_date", "diagnosis"}
	default:
		return nil
	}
}

func getECommerceDSCSVHeaders(tableName string) []string {
	switch tableName {
	case "dim_items":
		return []string{"i_item_sk", "i_item_id", "i_rec_start_date", "i_rec_end_date", "i_item_desc", "i_current_price", "i_wholesale_cost", "i_brand_id", "i_brand", "i_class_id", "i_class", "i_category_id", "i_category", "i_manufact_id", "i_manufact", "i_size", "i_formulation", "i_color", "i_units", "i_container", "i_manager_id", "i_product_name"}
	case "dim_customers":
		return []string{"c_customer_sk", "c_customer_id", "c_current_cdemo_sk", "c_current_hdemo_sk", "c_current_addr_sk", "c_first_shipto_date_sk", "c_first_sales_date_sk", "c_salutation", "c_first_name", "c_last_name", "c_preferred_cust_flag", "c_birth_day", "c_birth_month", "c_birth_year", "c_birth_country", "c_login", "c_email_address", "c_last_review_date_sk"}
	case "dim_customer_addresses":
		return []string{"ca_address_sk", "ca_address_id", "ca_address_date_sk", "ca_street_number", "ca_street_name", "ca_street_type", "ca_suite_number", "ca_city", "ca_county", "ca_state", "ca_zip", "ca_country", "ca_gmt_offset", "ca_location_type"}
	case "dim_customer_demographics":
		return []string{"cd_demo_sk", "cd_gender", "cd_marital_status", "cd_education_status", "cd_purchase_estimate", "cd_credit_rating", "cd_dep_count", "cd_dep_employed_count", "cd_dep_college_count", "cd_household_size", "cd_average_yearly_income", "cd_customer_segment"}
	case "dim_household_demographics":
		return []string{"hd_demo_sk", "hd_income_band_sk", "hd_buy_potential", "hd_dep_count", "hd_vehicle_count"}
	case "dim_promotions":
		return []string{"p_promo_sk", "p_promo_id", "p_start_date_sk", "p_end_date_sk", "p_item_sk", "p_cost", "p_target_market_class", "p_promo_name", "p_channel_dmail", "p_channel_email", "p_channel_catalog", "p_channel_tv", "p_channel_radio", "p_channel_press", "p_channel_event", "p_channel_demo", "p_purpose", "p_discount_active"}
	case "dim_stores":
		return []string{"s_store_sk", "s_store_id", "s_store_name", "s_store_number", "s_street_number", "s_street_name", "s_street_type", "s_suite_number", "s_city", "s_county", "s_state", "s_zip", "s_country", "s_gmt_offset", "s_tax_precentage", "s_floor_space", "s_hours", "s_manager", "s_market_id", "s_geography_class", "s_market_desc", "s_market_manager", "s_division_id", "s_division_name", "s_company_id", "s_company_name"}
	case "dim_call_centers":
		return []string{"cc_call_center_sk", "cc_call_center_id", "cc_rec_start_date", "cc_rec_end_date", "cc_closed_date_sk", "cc_open_date_sk", "cc_name", "cc_class", "cc_employees", "cc_sq_ft", "cc_hours", "cc_manager", "cc_mkt_id", "cc_mkt_class", "cc_mkt_desc", "cc_market_manager", "cc_division", "cc_division_name", "cc_company", "cc_company_name", "cc_street_number", "cc_street_name", "cc_street_type", "cc_suite_number", "cc_city", "cc_county", "cc_state", "cc_zip", "cc_country", "cc_gmt_offset", "cc_tax_percentage"}
	case "dim_catalog_pages":
		return []string{"cp_catalog_page_sk", "cp_catalog_page_id", "cp_start_date_sk", "cp_end_date_sk", "cp_department", "cp_catalog_number", "cp_catalog_page_number", "cp_description", "cp_type"}
	case "dim_web_sites":
		return []string{"web_site_sk", "web_site_id", "web_rec_start_date", "web_rec_end_date", "web_name", "web_open_date_sk", "web_close_date_sk", "web_class", "web_manager", "web_mkt_id", "web_mkt_class", "web_mkt_desc", "web_market_manager", "web_company_id", "web_company_name", "web_street_number", "web_street_name", "web_street_type", "web_suite_number", "web_city", "web_county", "web_state", "web_zip", "web_country", "web_gmt_offset", "web_tax_percentage"}
	case "dim_web_pages":
		return []string{"wp_web_page_sk", "wp_web_page_id", "wp_rec_start_date", "wp_rec_end_date", "wp_creation_date_sk", "wp_access_date_sk", "wp_autogen_flag", "wp_customer_sk", "wp_url", "wp_type", "wp_char_count", "wp_link_count", "wp_image_count", "wp_max_ad_count"}
	case "dim_warehouses":
		return []string{"w_warehouse_sk", "w_warehouse_id", "w_warehouse_name", "w_warehouse_sq_ft", "w_street_number", "w_street_name", "w_street_type", "w_suite_number", "w_city", "w_county", "w_state", "w_zip", "w_country", "w_gmt_offset", "w_tax_percentage"}
	case "dim_reasons":
		return []string{"r_reason_sk", "r_reason_id", "r_reason_desc"}
	case "dim_ship_modes":
		return []string{"sm_ship_mode_sk", "sm_ship_mode_id", "sm_type", "sm_code", "sm_carrier", "sm_contract"}
	case "dim_income_bands":
		return []string{"ib_income_band_sk", "ib_lower_bound", "ib_upper_bound"}
	case "dim_time":
		return []string{"t_time_sk", "t_time_id", "t_time", "t_hour", "t_minute", "t_second", "t_timezone_id", "t_am_pm", "t_shift", "t_sub_shift", "t_meal_time"}
	case "dim_date":
		return []string{"d_date_sk", "d_date_id", "d_date", "d_month_seq", "d_week_seq", "d_quarter_seq", "d_year", "d_dow", "d_moy", "d_dom", "d_qoy", "d_fy_year", "d_fy_quarter_seq", "d_fy_week_seq", "d_day_name", "d_quarter_name", "d_holiday", "d_weekend", "d_following_holiday", "d_first_dom", "d_last_dom", "d_same_day_ly", "d_same_day_lq", "d_current_day", "d_current_week", "d_current_month", "d_current_quarter", "d_current_year"}
	case "fact_store_sales":
		return []string{"ss_sold_date_sk", "ss_sold_time_sk", "ss_item_sk", "ss_customer_sk", "ss_cdemo_sk", "ss_hdemo_sk", "ss_addr_sk", "ss_store_sk", "ss_promo_sk", "ss_ticket_number", "ss_quantity", "ss_wholesale_cost", "ss_list_price", "ss_sales_price", "ss_ext_discount_amt", "ss_ext_sales_price", "ss_ext_wholesale_cost", "ss_ext_list_price", "ss_ext_tax", "ss_coupon_amt", "ss_net_paid", "ss_net_paid_inc_tax", "ss_net_profit"}
	case "fact_catalog_sales":
		return []string{"cs_sold_date_sk", "cs_sold_time_sk", "cs_ship_date_sk", "cs_bill_customer_sk", "cs_bill_cdemo_sk", "cs_bill_hdemo_sk", "cs_bill_addr_sk", "cs_ship_customer_sk", "cs_ship_cdemo_sk", "cs_ship_hdemo_sk", "cs_ship_addr_sk", "cs_call_center_sk", "cs_catalog_page_sk", "cs_ship_mode_sk", "cs_warehouse_sk", "cs_item_sk", "cs_promo_sk", "cs_order_number", "cs_quantity", "cs_wholesale_cost", "cs_list_price", "cs_sales_price", "cs_ext_discount_amt", "cs_ext_sales_price", "cs_ext_wholesale_cost", "cs_ext_list_price", "cs_ext_tax", "cs_coupon_amt", "cs_ext_ship_cost", "cs_net_paid", "cs_net_paid_inc_tax", "cs_net_paid_inc_ship", "cs_net_paid_inc_ship_tax", "cs_net_profit"}
	case "fact_web_sales":
		return []string{"ws_sold_date_sk", "ws_sold_time_sk", "ws_ship_date_sk", "ws_item_sk", "ws_bill_customer_sk", "ws_bill_cdemo_sk", "ws_bill_hdemo_sk", "ws_bill_addr_sk", "ws_ship_customer_sk", "ws_ship_cdemo_sk", "ws_ship_hdemo_sk", "ws_ship_addr_sk", "ws_web_page_sk", "ws_web_site_sk", "ws_ship_mode_sk", "ws_warehouse_sk", "ws_promo_sk", "ws_order_number", "ws_quantity", "ws_wholesale_cost", "ws_list_price", "ws_sales_price", "ws_ext_discount_amt", "ws_ext_sales_price", "ws_ext_wholesale_cost", "ws_ext_list_price", "ws_ext_tax", "ws_coupon_amt", "ws_ext_ship_cost", "ws_net_paid", "ws_net_paid_inc_tax", "ws_net_paid_inc_ship", "ws_net_paid_inc_ship_tax", "ws_net_profit"}
	default:
		return nil
	}
}

func getExpectedParquetColumns(model, tableName string) []string {
	switch model {
	case "ecommerce":
		return getECommerceParquetColumns(tableName)
	case "ecommerce-ds":
		return getECommerceDSCSVHeaders(tableName)
	case "financial":
		return getFinancialCSVHeaders(tableName)
	case "medical":
		return getMedicalCSVHeaders(tableName)
	default:
		return nil
	}
}

func getECommerceParquetColumns(tableName string) []string {
	switch tableName {
	case "dim_customers":
		return []string{"customer_id", "first_name", "last_name", "email"}
	case "dim_customer_addresses":
		return []string{"address_id", "customer_id", "address_type", "address", "city", "state", "zip", "country"}
	case "dim_suppliers":
		return []string{"supplier_id", "supplier_name", "country"}
	case "dim_product_categories":
		return []string{"category_id", "category_name"}
	case "dim_products":
		return []string{"product_id", "supplier_id", "product_name", "category_id", "base_price"}
	case "fact_orders_header":
		return []string{"order_id", "customer_id", "shipping_address_id", "billing_address_id", "order_timestamp", "order_status"}
	case "fact_order_items":
		return []string{"order_item_id", "order_id", "product_id", "quantity", "unit_price", "discount"}
	default:
		return nil
	}
}

func getTotalDirSize(dir string) int64 {
	var total int64
	filepath.Walk(dir, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			total += info.Size()
		}
		return nil
	})
	return total
}
