// cmd/e2e_test.go
package tests

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"io"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"testing"
	"time"
)

// TestE2EEcommerceGeneration performs an end-to-end test of the data generation process for the ecommerce model.
func TestE2EEcommerceGeneration(t *testing.T) {
	// 1. Setup: Build the binary and create a temporary output directory
	t.Log("Building Gengo binary...")
	buildCmd := exec.Command("go", "build", "-o", "Gengo_test", "..")
	if err := buildCmd.Run(); err != nil {
		t.Fatalf("Failed to build Gengo binary: %v", err)
	}

	// Add execute permissions to the binary
	if err := os.Chmod("./Gengo_test", 0755); err != nil {
		t.Fatalf("Failed to set executable permission on Gengo_test: %v", err)
	}

	rand.Seed(time.Now().UnixNano())
	tempDir := fmt.Sprintf("gengo_test_output_%d", rand.Intn(100000))
	t.Logf("Using temporary directory: %s", tempDir)

	// 2. Execution: Run the generator with predefined inputs
	genCmd := exec.Command("./Gengo_test", "gen")
	genCmd.Env = append(os.Environ(), "GENGO_TEST_MODE=true")
	stdin, err := genCmd.StdinPipe()
	if err != nil {
		t.Fatalf("Failed to get stdin pipe: %v", err)
	}

	go func() {
		defer stdin.Close()
		// Inputs: model type, very small size, csv format, temp directory name
		io.WriteString(stdin, "ecommerce\n")
		io.WriteString(stdin, "0.1\n")
		io.WriteString(stdin, "csv\n")
		io.WriteString(stdin, tempDir+"\n")
	}()

	t.Log("Running Gengo generator...")
	var stdout, stderr bytes.Buffer
	genCmd.Stdout = &stdout
	genCmd.Stderr = &stderr

	err = genCmd.Run()

	t.Logf("Gengo stdout:\n%s", stdout.String())
	if err != nil {
		t.Logf("Gengo stderr:\n%s", stderr.String())
		t.Fatalf("Gengo command failed: %v", err)
	}
	t.Log("Generator finished successfully.")

	// 3. Verification: Defer cleanup and run checks
	defer func() {
		t.Logf("Cleaning up temporary directory: %s", tempDir)
		os.RemoveAll(tempDir)
		os.Remove("./Gengo_test")
	}()

	// --- Read all generated data ---
	t.Log("Reading generated files...")
	customers := readCsvFile(t, filepath.Join(tempDir, "dim_customers.csv"))
	addresses := readCsvFile(t, filepath.Join(tempDir, "dim_customer_addresses.csv"))
	suppliers := readCsvFile(t, filepath.Join(tempDir, "dim_suppliers.csv"))
	products := readCsvFile(t, filepath.Join(tempDir, "dim_products.csv"))
	headers := readCsvFile(t, filepath.Join(tempDir, "fact_orders_header.csv"))
	items := readShardedCsvFiles(t, tempDir, "fact_order_items")

	// --- Create maps for FK lookups ---
	customerPKs := createPrimaryKeySet(t, customers, "customer_id")
	addressPKs := createPrimaryKeySet(t, addresses, "address_id")
	supplierPKs := createPrimaryKeySet(t, suppliers, "supplier_id")
	productPKs := createPrimaryKeySet(t, products, "product_id")
	orderHeaderPKs := createPrimaryKeySet(t, headers, "order_id")

	// Create a map for customer_id -> address_id[]
	customerAddressMap := make(map[string][]string)
	for _, addr := range addresses[1:] { // Skip header
		customerID := addr[1]
		addressID := addr[0]
		customerAddressMap[customerID] = append(customerAddressMap[customerID], addressID)
	}

	// --- Perform Integrity Checks ---
	t.Log("Verifying data integrity...")

	// Check FKs in dim_customer_addresses
	verifyForeignKey(t, addresses, "customer_id", customerPKs, "dim_customers")

	// Check FKs in dim_products
	verifyForeignKey(t, products, "supplier_id", supplierPKs, "dim_suppliers")

	// Check FKs in fact_orders_header
	verifyForeignKey(t, headers, "customer_id", customerPKs, "dim_customers")
	verifyForeignKey(t, headers, "shipping_address_id", addressPKs, "dim_customer_addresses")
	verifyForeignKey(t, headers, "billing_address_id", addressPKs, "dim_customer_addresses")

	// Check FKs in fact_order_items
	verifyForeignKey(t, items, "order_id", orderHeaderPKs, "fact_orders_header")
	verifyForeignKey(t, items, "product_id", productPKs, "dim_products")

	// Check customer-address consistency in headers
	headerMap := createRecordMap(headers)
	for _, header := range headers[1:] {
		customerID := header[headerMap["customer_id"]]
		shippingID := header[headerMap["shipping_address_id"]]
		billingID := header[headerMap["billing_address_id"]]

		validShipping := false
		for _, addrID := range customerAddressMap[customerID] {
			if addrID == shippingID {
				validShipping = true
				break
			}
		}
		if !validShipping {
			t.Errorf("Order %s has shipping_address_id %s which does not belong to customer %s", header[0], shippingID, customerID)
		}

		validBilling := false
		for _, addrID := range customerAddressMap[customerID] {
			if addrID == billingID {
				validBilling = true
				break
			}
		}
		if !validBilling {
			t.Errorf("Order %s has billing_address_id %s which does not belong to customer %s", header[0], billingID, customerID)
		}
	}

	t.Log("All ecommerce integrity checks passed.")
}

// TestE2EFinancialGeneration performs an end-to-end test of the data generation process for the financial model.
func TestE2EFinancialGeneration(t *testing.T) {
	// 1. Setup: Build the binary and create a temporary output directory
	t.Log("Building Gengo binary...")
	buildCmd := exec.Command("go", "build", "-o", "Gengo_test", "..")
	if err := buildCmd.Run(); err != nil {
		t.Fatalf("Failed to build Gengo binary: %v", err)
	}

	// Add execute permissions to the binary
	if err := os.Chmod("./Gengo_test", 0755); err != nil {
		t.Fatalf("Failed to set executable permission on Gengo_test: %v", err)
	}

	rand.Seed(time.Now().UnixNano())
	tempDir := fmt.Sprintf("gengo_test_output_financial_%d", rand.Intn(100000))
	t.Logf("Using temporary directory: %s", tempDir)

	// 2. Execution: Run the generator with predefined inputs
	genCmd := exec.Command("./Gengo_test", "gen")
	genCmd.Env = append(os.Environ(), "GENGO_TEST_MODE=true")
	stdin, err := genCmd.StdinPipe()
	if err != nil {
		t.Fatalf("Failed to get stdin pipe: %v", err)
	}

	go func() {
		defer stdin.Close()
		// Inputs: model type, very small size, csv format, temp directory name
		io.WriteString(stdin, "financial\n")
		io.WriteString(stdin, "0.1\n")
		io.WriteString(stdin, "csv\n")
		io.WriteString(stdin, tempDir+"\n")
	}()

	t.Log("Running Gengo financial generator...")
	var stdout, stderr bytes.Buffer
	genCmd.Stdout = &stdout
	genCmd.Stderr = &stderr

	err = genCmd.Run()

	t.Logf("Gengo stdout:\n%s", stdout.String())
	if err != nil {
		t.Logf("Gengo stderr:\n%s", stderr.String())
		t.Fatalf("Gengo financial command failed: %v", err)
	}
	t.Log("Financial Generator finished successfully.")

	// 3. Verification: Defer cleanup and run checks
	defer func() {
		t.Logf("Cleaning up temporary directory: %s", tempDir)
		os.RemoveAll(tempDir)
		os.Remove("./Gengo_test")
	}()

	// --- Read all generated data ---
	t.Log("Reading generated financial files...")
	companies := readCsvFile(t, filepath.Join(tempDir, "dim_companies.csv"))
	exchanges := readCsvFile(t, filepath.Join(tempDir, "dim_exchanges.csv"))
	stockPrices := readCsvFile(t, filepath.Join(tempDir, "fact_daily_stock_prices.csv"))

	// --- Create maps for FK lookups ---
	companyPKs := createPrimaryKeySet(t, companies, "company_id")
	exchangePKs := createPrimaryKeySet(t, exchanges, "exchange_id")

	// --- Perform Integrity Checks ---
	t.Log("Verifying financial data integrity...")

	// Check FKs in fact_daily_stock_prices
	verifyForeignKey(t, stockPrices, "company_id", companyPKs, "dim_companies")
	verifyForeignKey(t, stockPrices, "exchange_id", exchangePKs, "dim_exchanges")

	// Verify 3NF by simulating a join
	verify3NFFinancial(t, companies, exchanges, stockPrices)

	t.Log("All financial integrity checks passed.")
}

// TestE2EMedicalGeneration performs an end-to-end test of the data generation process for the medical model.
func TestE2EMedicalGeneration(t *testing.T) {
	// 1. Setup: Build the binary and create a temporary output directory
	t.Log("Building Gengo binary...")
	buildCmd := exec.Command("go", "build", "-o", "Gengo_test", "..")
	if err := buildCmd.Run(); err != nil {
		t.Fatalf("Failed to build Gengo binary: %v", err)
	}

	// Add execute permissions to the binary
	if err := os.Chmod("./Gengo_test", 0755); err != nil {
		t.Fatalf("Failed to set executable permission on Gengo_test: %v", err)
	}

	rand.Seed(time.Now().UnixNano())
	tempDir := fmt.Sprintf("gengo_test_output_medical_%d", rand.Intn(100000))
	t.Logf("Using temporary directory: %s", tempDir)

	// 2. Execution: Run the generator with predefined inputs
	genCmd := exec.Command("./Gengo_test", "gen")
	genCmd.Env = append(os.Environ(), "GENGO_TEST_MODE=true")
	stdin, err := genCmd.StdinPipe()
	if err != nil {
		t.Fatalf("Failed to get stdin pipe: %v", err)
	}

	go func() {
		defer stdin.Close()
		// Inputs: model type, very small size, csv format, temp directory name
		io.WriteString(stdin, "medical\n")
		io.WriteString(stdin, "0.1\n")
		io.WriteString(stdin, "csv\n")
		io.WriteString(stdin, tempDir+"\n")
	}()

	t.Log("Running Gengo medical generator...")
	var stdout, stderr bytes.Buffer
	genCmd.Stdout = &stdout
	genCmd.Stderr = &stderr

	err = genCmd.Run()

	t.Logf("Gengo stdout:\n%s", stdout.String())
	if err != nil {
		t.Logf("Gengo stderr:\n%s", stderr.String())
		t.Fatalf("Gengo medical command failed: %v", err)
	}
	t.Log("Medical Generator finished successfully.")

	// 3. Verification: Defer cleanup and run checks
	defer func() {
		t.Logf("Cleaning up temporary directory: %s", tempDir)
		os.RemoveAll(tempDir)
		os.Remove("./Gengo_test")
	}()

	// --- Read all generated data ---
	t.Log("Reading generated medical files...")
	patients := readCsvFile(t, filepath.Join(tempDir, "dim_patients.csv"))
	doctors := readCsvFile(t, filepath.Join(tempDir, "dim_doctors.csv"))
	clinics := readCsvFile(t, filepath.Join(tempDir, "dim_clinics.csv"))
	appointments := readCsvFile(t, filepath.Join(tempDir, "fact_appointments.csv"))

	// --- Create maps for FK lookups ---
	patientPKs := createPrimaryKeySet(t, patients, "patient_id")
	doctorPKs := createPrimaryKeySet(t, doctors, "doctor_id")
	clinicPKs := createPrimaryKeySet(t, clinics, "clinic_id")

	// --- Perform Integrity Checks ---
	t.Log("Verifying medical data integrity...")

	// Check FKs in fact_appointments
	verifyForeignKey(t, appointments, "patient_id", patientPKs, "dim_patients")
	verifyForeignKey(t, appointments, "doctor_id", doctorPKs, "dim_doctors")
	verifyForeignKey(t, appointments, "clinic_id", clinicPKs, "dim_clinics")

	t.Log("All medical integrity checks passed.")
}

// verify3NFFinancial checks the 3NF integrity of the financial data by simulating a join.
func verify3NFFinancial(t *testing.T, companies, exchanges, stockPrices [][]string) {
	t.Log("Verifying 3NF for financial data by simulating a join...")

	// Create maps for dim_companies and dim_exchanges for easy lookup
	companiesMap := make(map[string][]string)
	companiesHeaderMap := createRecordMap(companies)
	companyIDIndex, ok := companiesHeaderMap["company_id"]
	if !ok {
		t.Fatal("column 'company_id' not found in dim_companies")
	}
	companyNameIndex, ok := companiesHeaderMap["company_name"]
	if !ok {
		t.Fatal("column 'company_name' not found in dim_companies")
	}
	for _, record := range companies[1:] {
		companiesMap[record[companyIDIndex]] = record
	}

	exchangesMap := make(map[string][]string)
	exchangesHeaderMap := createRecordMap(exchanges)
	exchangeIDIndex, ok := exchangesHeaderMap["exchange_id"]
	if !ok {
		t.Fatal("column 'exchange_id' not found in dim_exchanges")
	}
	exchangeNameIndex, ok := exchangesHeaderMap["exchange_name"]
	if !ok {
		t.Fatal("column 'exchange_name' not found in dim_exchanges")
	}
	for _, record := range exchanges[1:] {
		exchangesMap[record[exchangeIDIndex]] = record
	}

	stockPricesHeaderMap := createRecordMap(stockPrices)
	stockCompanyIDIndex, ok := stockPricesHeaderMap["company_id"]
	if !ok {
		t.Fatal("column 'company_id' not found in fact_daily_stock_prices")
	}
	stockExchangeIDIndex, ok := stockPricesHeaderMap["exchange_id"]
	if !ok {
		t.Fatal("column 'exchange_id' not found in fact_daily_stock_prices")
	}

	// "Join" fact_daily_stock_prices with dim_companies and dim_exchanges
	for i, stockRecord := range stockPrices[1:] {
		companyID := stockRecord[stockCompanyIDIndex]
		exchangeID := stockRecord[stockExchangeIDIndex]

		// Check if company exists
		companyRecord, ok := companiesMap[companyID]
		if !ok {
			// This is already checked by verifyForeignKey, but it's good practice for a join simulation
			t.Errorf("Row %d in fact_daily_stock_prices: Company with ID '%s' not found in dim_companies", i+2, companyID)
			continue
		}

		// Check if exchange exists
		exchangeRecord, ok := exchangesMap[exchangeID]
		if !ok {
			// This is already checked by verifyForeignKey, but it's good practice for a join simulation
			t.Errorf("Row %d in fact_daily_stock_prices: Exchange with ID '%s' not found in dim_exchanges", i+2, exchangeID)
			continue
		}

		// 3NF check: ensure some data from the joined tables is consistent and not null.
		if companyRecord[companyNameIndex] == "" {
			t.Errorf("Row %d in fact_daily_stock_prices: Joined company with ID '%s' has an empty company_name", i+2, companyID)
		}

		if exchangeRecord[exchangeNameIndex] == "" {
			t.Errorf("Row %d in fact_daily_stock_prices: Joined exchange with ID '%s' has an empty exchange_name", i+2, exchangeID)
		}
	}

	t.Log("3NF verification for financial data passed.")
}

// --- Test Helper Functions ---

// readShardedCsvFiles reads multiple sharded CSV files and combines them into a single dataset.
// It looks for files matching the pattern: {baseName}_*.csv (e.g., fact_order_items_0.csv, fact_order_items_1.csv)
// If no sharded files exist, it falls back to trying a single file: {baseName}.csv
func readShardedCsvFiles(t *testing.T, dir string, baseName string) [][]string {
	// First, try to find sharded files
	files, err := filepath.Glob(filepath.Join(dir, baseName+"_*.csv"))
	if err != nil {
		t.Fatalf("Failed to glob for sharded files %s_*.csv: %v", baseName, err)
	}

	if len(files) == 0 {
		// No sharded files found, try single file
		singleFile := filepath.Join(dir, baseName+".csv")
		if _, err := os.Stat(singleFile); err == nil {
			t.Logf("No sharded files found, using single file: %s", singleFile)
			return readCsvFile(t, singleFile)
		}
		t.Fatalf("No sharded files (%s_*.csv) or single file (%s.csv) found in %s", baseName, baseName, dir)
	}

	// Sort files to ensure consistent order (fact_order_items_0.csv, fact_order_items_1.csv, etc.)
	sort.Strings(files)
	t.Logf("Found %d sharded files for %s: %v", len(files), baseName, files)

	var allRecords [][]string
	var header []string

	for i, file := range files {
		records := readCsvFile(t, file)
		if len(records) == 0 {
			continue // Skip empty files
		}

		if i == 0 {
			// First file: keep header and all records
			header = records[0]
			allRecords = append(allRecords, records...)
		} else {
			// Subsequent files: verify header matches and append data rows only
			if len(records[0]) != len(header) {
				t.Fatalf("Header mismatch in file %s: expected %d columns, got %d", file, len(header), len(records[0]))
			}
			for j, col := range records[0] {
				if col != header[j] {
					t.Fatalf("Header mismatch in file %s: column %d expected '%s', got '%s'", file, j, header[j], col)
				}
			}
			// Append data rows (skip header)
			allRecords = append(allRecords, records[1:]...)
		}
	}

	t.Logf("Combined %d files into %d total records (including header)", len(files), len(allRecords))
	return allRecords
}

func readCsvFile(t *testing.T, path string) [][]string {
	file, err := os.Open(path)
	if err != nil {
		t.Fatalf("Failed to open file %s: %v", path, err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		t.Fatalf("Failed to read CSV data from %s: %v", path, err)
	}
	return records
}

func createPrimaryKeySet(t *testing.T, records [][]string, pkColumnName string) map[string]struct{} {
	if len(records) < 2 {
		return make(map[string]struct{}) // Empty or header-only file
	}
	rmap := createRecordMap(records)
	pkIndex, ok := rmap[pkColumnName]
	if !ok {
		t.Fatalf("Primary key column '%s' not found in records", pkColumnName)
	}

	pkSet := make(map[string]struct{}, len(records)-1)
	for i, record := range records[1:] { // Skip header
		pk := record[pkIndex]
		if _, exists := pkSet[pk]; exists {
			t.Errorf("Duplicate primary key '%s' found in column '%s' at row %d", pk, pkColumnName, i+2)
		}
		pkSet[pk] = struct{}{}
	}
	return pkSet
}

func verifyForeignKey(t *testing.T, records [][]string, fkColumnName string, pkSet map[string]struct{}, targetTable string) {
	if len(records) < 2 {
		return // Nothing to verify
	}
	rmap := createRecordMap(records)
	fkIndex, ok := rmap[fkColumnName]
	if !ok {
		t.Fatalf("Foreign key column '%s' not found in records", fkColumnName)
	}

	for i, record := range records[1:] { // Skip header
		fk := record[fkIndex]
		if _, exists := pkSet[fk]; !exists {
			t.Errorf("Foreign key violation: value '%s' from column '%s' (row %d) not found in target table '%s'", fk, fkColumnName, i+2, targetTable)
		}
	}
}

func createRecordMap(records [][]string) map[string]int {
	rmap := make(map[string]int)
	if len(records) > 0 {
		for i, header := range records[0] {
			rmap[header] = i
		}
	}
	return rmap
}