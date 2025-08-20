package core

import (
	"fmt"
	"path/filepath"
	"os"
	"sync"
	"time"

	"github.com/peekknuf/Gengo/internal/formats"
	ecommercemodels "github.com/peekknuf/Gengo/internal/models/ecommerce"
	financialmodels "github.com/peekknuf/Gengo/internal/models/financial"
	medicalmodels "github.com/peekknuf/Gengo/internal/models/medical"
	"github.com/peekknuf/Gengo/internal/simulation/ecommerce"
	financialsimulation "github.com/peekknuf/Gengo/internal/simulation/financial"
	medicalsimulation "github.com/peekknuf/Gengo/internal/simulation/medical"
)

// GenerateModelData orchestrates the generation and writing of the relational model.
func GenerateModelData(modelType string, counts interface{}, format string, outputDir string) error {
	startTime := time.Now()

	err := os.MkdirAll(outputDir, 0755)
	if err != nil {
		return fmt.Errorf("error creating output directory %s: %w", outputDir, err)
	}
	fmt.Printf("Ensured output directory exists: %s\n", outputDir)

	switch modelType {
	case "ecommerce":
		err = generateECommerceDataConcurrently(counts.(ECommerceRowCounts), format, outputDir)
	case "financial":
		err = generateFinancialDataConcurrently(counts.(financialsimulation.FinancialRowCounts), format, outputDir)
	case "medical":
		err = generateMedicalDataConcurrently(counts.(medicalsimulation.MedicalRowCounts), format, outputDir)
	default:
		err = fmt.Errorf("unsupported model type: %s", modelType)
	}

	if err != nil {
		return err
	}

	fmt.Printf("\nTotal model generation completed in %s.\n", time.Since(startTime).Round(time.Second))
	return nil
}

func generateECommerceDataConcurrently(counts ECommerceRowCounts, format string, outputDir string) error {
	var writersWg sync.WaitGroup
	errChan := make(chan error, 10)

	// --- Dimension Data Holders ---
	var customers []ecommercemodels.Customer
	var suppliers []ecommercemodels.Supplier
	var customerAddresses []ecommercemodels.CustomerAddress
	var products []ecommercemodels.Product
	var productCategories []ecommercemodels.ProductCategory

	// --- Fact Table Pipeline Setup ---
	// Channels for byte chunks instead of structs
	headersChunkChan := make(chan []byte, 100)
	itemsChunkChan := make(chan []byte, 100)

	// Launch Fact Writers Immediately
	writersWg.Add(2)
	go func() {
		defer writersWg.Done()
		target := filepath.Join(outputDir, "fact_orders_header.csv")
		header := "order_id,customer_id,shipping_address_id,billing_address_id,order_timestamp_unix,order_status"
		if err := formats.WriteCSVChunks(header, headersChunkChan, target); err != nil {
			errChan <- err
		}
	}()
	go func() {
		defer writersWg.Done()
		target := filepath.Join(outputDir, "fact_order_items.csv")
		header := "order_item_id,order_id,product_id,quantity,unit_price,discount,total_price"
		if err := formats.WriteCSVChunks(header, itemsChunkChan, target); err != nil {
			errChan <- err
		}
	}()

	// --- Concurrent Dimension Generation with Correct Parallelism ---
	var customersWg, suppliersWg, categoriesWg, productsWg sync.WaitGroup
	customersWg.Add(1)
	suppliersWg.Add(1)
	categoriesWg.Add(1)
	productsWg.Add(1)

	go func() {
		defer customersWg.Done()
		customers = ecommerce.GenerateCustomers(counts.Customers)
		customerAddresses = ecommerce.GenerateCustomerAddresses(customers)
	}()
	go func() {
		defer suppliersWg.Done()
		suppliers = ecommerce.GenerateSuppliers(counts.Suppliers)
	}()
	go func() {
		defer categoriesWg.Done()
		productCategories = ecommerce.GenerateProductCategories()
	}()
	go func() {
		defer productsWg.Done()
		suppliersWg.Wait()
		categoriesWg.Wait()
		supplierIDs := make([]int, len(suppliers))
		for i, s := range suppliers {
			supplierIDs[i] = s.SupplierID
		}
		categoryIDs := make([]int, len(productCategories))
		for i, pc := range productCategories {
			categoryIDs[i] = pc.CategoryID
		}
		products = ecommerce.GenerateProducts(counts.Products, supplierIDs, categoryIDs)
	}()

	// --- Concurrent Dimension Writing ---
	writersWg.Add(5)
	go func() {
		defer writersWg.Done()
		customersWg.Wait()
		if err := formats.WriteCustomersToCSV(customers, filepath.Join(outputDir, "dim_customers.csv")); err != nil {
			errChan <- err
		}
	}()
	go func() {
		defer writersWg.Done()
		customersWg.Wait()
		if err := formats.WriteCustomerAddressesToCSV(customerAddresses, filepath.Join(outputDir, "dim_customer_addresses.csv")); err != nil {
			errChan <- err
		}
	}()
	go func() {
		defer writersWg.Done()
		suppliersWg.Wait()
		if err := formats.WriteSuppliersToCSV(suppliers, filepath.Join(outputDir, "dim_suppliers.csv")); err != nil {
			errChan <- err
		}
	}()
	go func() {
		defer writersWg.Done()
		categoriesWg.Wait()
		if err := formats.WriteProductCategoriesToCSV(productCategories, filepath.Join(outputDir, "dim_product_categories.csv")); err != nil {
			errChan <- err
		}
	}()
	go func() {
		defer writersWg.Done()
		productsWg.Wait()
		if err := formats.WriteProductsToCSV(products, filepath.Join(outputDir, "dim_products.csv")); err != nil {
			errChan <- err
		}
	}()

	// --- Fact Generation ---
	// Start fact generation as soon as we have the required data, not waiting for dimension files to be written
	go func() {
		// Wait for customer and product data to be ready
		customersWg.Wait()
		productsWg.Wait()
		
		// Prepare data for fact generation
		customerIDs := make([]int, len(customers))
		for i, c := range customers {
			customerIDs[i] = c.CustomerID
		}
		
		// Build slice instead of map for product details (optimization #5)
		maxProductID := 0
		for _, p := range products {
			if p.ProductID > maxProductID {
				maxProductID = p.ProductID
			}
		}
		productDetails := make([]ecommercemodels.ProductDetails, maxProductID+1)
		productIDsForSampling := make([]int, len(products))
		for i, p := range products {
			productDetails[p.ProductID] = ecommercemodels.ProductDetails{BasePrice: p.BasePrice}
			productIDsForSampling[i] = p.ProductID
		}
		
		// Generate facts immediately without waiting for dimension files to be written
		if err := ecommerce.GenerateECommerceModelData(counts.OrderHeaders, customerIDs, customerAddresses, productDetails, productIDsForSampling, headersChunkChan, itemsChunkChan); err != nil {
			errChan <- err
		}
	}()

	// --- Final Synchronization ---
	writersWg.Wait()
	close(errChan)

	for err := range errChan {
		if err != nil {
			return err
		}
	}
	return nil
}

func generateFinancialDataConcurrently(counts financialsimulation.FinancialRowCounts, format string, outputDir string) error {
	var wg sync.WaitGroup
	errChan := make(chan error, 3) // 2 dims + 1 fact operation

	// --- Concurrent Dimension Generation ---
	var companies []financialmodels.Company
	var exchanges []financialmodels.Exchange

	var genWg sync.WaitGroup
	genWg.Add(2)

	go func() {
		defer genWg.Done()
		companies = financialsimulation.GenerateCompanies(counts.Companies)
	}()

	go func() {
		defer genWg.Done()
		exchanges = financialsimulation.GenerateExchanges(counts.Exchanges)
	}()

	genWg.Wait()

	// --- Concurrent Dimension Writing ---
	wg.Add(2)
	go func() {
		defer wg.Done()
		errChan <- formats.WriteSliceData(companies, "dim_companies", format, outputDir)
	}()
	go func() {
		defer wg.Done()
		errChan <- formats.WriteSliceData(exchanges, "dim_exchanges", format, outputDir)
	}()

	// --- Fact Table Generation and Writing ---
	// This runs after dimension generation is complete.
	// The financial simulation function handles its own writing.
	err := financialsimulation.GenerateFinancialModelData(counts, companies, exchanges, format, outputDir)
	if err != nil {
		errChan <- err
	}

	wg.Wait()
	close(errChan)

	for err := range errChan {
		if err != nil {
			return err
		}
	}

	return nil
}

func generateMedicalDataConcurrently(counts medicalsimulation.MedicalRowCounts, format string, outputDir string) error {
	var wg sync.WaitGroup
	errChan := make(chan error, 4) // 3 dims + 1 fact operation

	// --- Concurrent Dimension Generation ---
	var patients []medicalmodels.Patient
	var doctors []medicalmodels.Doctor
	var clinics []medicalmodels.Clinic

	var genWg sync.WaitGroup
	genWg.Add(3)

	go func() {
		defer genWg.Done()
		patients = medicalsimulation.GeneratePatients(counts.Patients)
	}()

	go func() {
		defer genWg.Done()
		doctors = medicalsimulation.GenerateDoctors(counts.Doctors)
	}()

	go func() {
		defer genWg.Done()
		clinics = medicalsimulation.GenerateClinics(counts.Clinics)
	}()

	genWg.Wait()

	// --- Concurrent Dimension Writing ---
	wg.Add(3)
	go func() {
		defer wg.Done()
		errChan <- formats.WriteSliceData(patients, "dim_patients", format, outputDir)
	}()
	go func() {
		defer wg.Done()
		errChan <- formats.WriteSliceData(doctors, "dim_doctors", format, outputDir)
	}()
	go func() {
		defer wg.Done()
		errChan <- formats.WriteSliceData(clinics, "dim_clinics", format, outputDir)
	}()

	// --- Fact Table Generation and Writing ---
	// This runs after dimension generation is complete.
	// The medical simulation function handles its own writing.
	err := medicalsimulation.GenerateMedicalModelData(counts, patients, doctors, clinics, format, outputDir)
	if err != nil {
		errChan <- err
	}

	wg.Wait()
	close(errChan)

	for err := range errChan {
		if err != nil {
			return err
		}
	}

	return nil
}