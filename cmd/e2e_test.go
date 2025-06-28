// cmd/e2e_test.go
package cmd

import (
	"encoding/csv"
	"fmt"
	"io"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
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
	if err := os.Chmod("Gengo_test", 0755); err != nil {
		t.Fatalf("Failed to set executable permission on Gengo_test: %v", err)
	}

	rand.Seed(time.Now().UnixNano())
	tempDir := fmt.Sprintf("gengo_test_output_%d", rand.Intn(100000))
	t.Logf("Using temporary directory: %s", tempDir)

	// 2. Execution: Run the generator with predefined inputs
	genCmd := exec.Command("./Gengo_test", "gen")
	stdin, err := genCmd.StdinPipe()
	if err != nil {
		t.Fatalf("Failed to get stdin pipe: %v", err)
	}

	go func() {
		defer stdin.Close()
		// Inputs: model type, very small size, csv format, temp directory name
		io.WriteString(stdin, "ecommerce\n")
		io.WriteString(stdin, "0.00001\n")
		io.WriteString(stdin, "csv\n")
		io.WriteString(stdin, tempDir+"\n")
	}()

	t.Log("Running Gengo generator...")
	output, err := genCmd.CombinedOutput()
	if err != nil {
		t.Logf("Generator output:\n%s", string(output))
		t.Fatalf("Gengo command failed: %v", err)
	}
	t.Log("Generator finished successfully.")

	// 3. Verification: Defer cleanup and run checks
	defer func() {
		t.Logf("Cleaning up temporary directory: %s", tempDir)
		os.RemoveAll(tempDir)
		os.Remove("Gengo_test")
	}()

	// --- Read all generated data ---
	t.Log("Reading generated files...")
	customers := readCsvFile(t, filepath.Join(tempDir, "dim_customers.csv"))
	addresses := readCsvFile(t, filepath.Join(tempDir, "dim_customer_addresses.csv"))
	suppliers := readCsvFile(t, filepath.Join(tempDir, "dim_suppliers.csv"))
	products := readCsvFile(t, filepath.Join(tempDir, "dim_products.csv"))
	headers := readCsvFile(t, filepath.Join(tempDir, "fact_orders_header.csv"))
	items := readCsvFile(t, filepath.Join(tempDir, "fact_order_items.csv"))

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
	if err := os.Chmod("Gengo_test", 0755); err != nil {
		t.Fatalf("Failed to set executable permission on Gengo_test: %v", err)
	}

	rand.Seed(time.Now().UnixNano())
	tempDir := fmt.Sprintf("gengo_test_output_financial_%d", rand.Intn(100000))
	t.Logf("Using temporary directory: %s", tempDir)

	// 2. Execution: Run the generator with predefined inputs
	genCmd := exec.Command("./Gengo_test", "gen")
	stdin, err := genCmd.StdinPipe()
	if err != nil {
		t.Fatalf("Failed to get stdin pipe: %v", err)
	}

	go func() {
		defer stdin.Close()
		// Inputs: model type, very small size, csv format, temp directory name
		io.WriteString(stdin, "financial\n")
		io.WriteString(stdin, "0.00001\n")
		io.WriteString(stdin, "csv\n")
		io.WriteString(stdin, tempDir+"\n")
	}()

	t.Log("Running Gengo financial generator...")
	output, err := genCmd.CombinedOutput()
	if err != nil {
		t.Logf("Financial Generator output:\n%s", string(output))
		t.Fatalf("Gengo financial command failed: %v", err)
	}
	t.Log("Financial Generator finished successfully.")

	// 3. Verification: Defer cleanup and run checks
	defer func() {
		t.Logf("Cleaning up temporary directory: %s", tempDir)
		os.RemoveAll(tempDir)
		os.Remove("Gengo_test")
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

	t.Log("All financial integrity checks passed.")
}

// --- Test Helper Functions ---

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

func createRecordMap(records [][]string) map[string]int {
	header := records[0]
	rmap := make(map[string]int, len(header))
	for i, h := range header {
		rmap[h] = i
	}
	return rmap
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