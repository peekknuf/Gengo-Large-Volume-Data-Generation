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
	var wg sync.WaitGroup
	errChan := make(chan error, 10)

	// --- Dimension Data Holders ---
	var customers []ecommercemodels.Customer
	var suppliers []ecommercemodels.Supplier
	var customerAddresses []ecommercemodels.CustomerAddress
	var products []ecommercemodels.Product
	var productCategories []ecommercemodels.ProductCategory

	// --- High-Performance Pipeline Setup ---
	// Create specific channels for item-by-item streaming
	headersChan := make(chan ecommercemodels.OrderHeader, 1000)
	itemsChan := make(chan ecommercemodels.OrderItem, 5000)

	// --- Goroutines for Writing Fact Tables (using high-performance writers) ---
	wg.Add(1)
	go func() {
		defer wg.Done()
		targetFilename := filepath.Join(outputDir, "fact_orders_header.csv")
		errChan <- formats.WriteStreamOrderHeadersToCSV(headersChan, targetFilename)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		targetFilename := filepath.Join(outputDir, "fact_order_items.csv")
		errChan <- formats.WriteStreamOrderItemsToCSV(itemsChan, targetFilename)
	}()

	// --- Concurrent Dimension Generation with Correct Parallelism ---
	var customersWg, suppliersWg, categoriesWg, productsWg sync.WaitGroup
	customersWg.Add(1)
	suppliersWg.Add(1)
	categoriesWg.Add(1)
	productsWg.Add(1)

	// Goroutine for Customers and Addresses
	go func() {
		defer customersWg.Done()
		customers = ecommerce.GenerateCustomers(counts.Customers)
		customerAddresses = ecommerce.GenerateCustomerAddresses(customers)
	}()

	// Goroutine for Suppliers
	go func() {
		defer suppliersWg.Done()
		suppliers = ecommerce.GenerateSuppliers(counts.Suppliers)
	}()

	// Goroutine for Product Categories
	go func() {
		defer categoriesWg.Done()
		productCategories = ecommerce.GenerateProductCategories()
	}()

	// Goroutine for Products (depends on Suppliers and Categories)
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

	// --- Goroutine to Start Fact Generation as soon as Core Dimensions are Ready ---
	wg.Add(1)
	go func() {
		defer wg.Done()
		customersWg.Wait()
		productsWg.Wait()

		customerIDs := make([]int, len(customers))
		for i, c := range customers {
			customerIDs[i] = c.CustomerID
		}
		productInfo := make(map[int]ecommercemodels.ProductDetails, len(products))
		productIDsForSampling := make([]int, len(products))
		for i, p := range products {
			productInfo[p.ProductID] = ecommercemodels.ProductDetails{BasePrice: p.BasePrice}
			productIDsForSampling[i] = p.ProductID
		}

		err := ecommerce.GenerateECommerceModelData(counts.OrderHeaders, customerIDs, customerAddresses, productInfo, productIDsForSampling, headersChan, itemsChan)
		if err != nil {
			errChan <- fmt.Errorf("error during fact table generation: %w", err)
		}
	}()

	// --- Concurrent Dimension Writing (using high-performance writers via dispatcher) ---
	wg.Add(5)
	go func() {
		defer wg.Done()
		customersWg.Wait()
		errChan <- formats.WriteSliceData(customers, "dim_customers", format, outputDir)
	}()
	go func() {
		defer wg.Done()
		customersWg.Wait() // Addresses are generated with customers
		errChan <- formats.WriteSliceData(customerAddresses, "dim_customer_addresses", format, outputDir)
	}()
	go func() {
		defer wg.Done()
		suppliersWg.Wait()
		errChan <- formats.WriteSliceData(suppliers, "dim_suppliers", format, outputDir)
	}()
	go func() {
		defer wg.Done()
		categoriesWg.Wait()
		errChan <- formats.WriteSliceData(productCategories, "dim_product_categories", format, outputDir)
	}()
	go func() {
		defer wg.Done()
		productsWg.Wait()
		errChan <- formats.WriteSliceData(products, "dim_products", format, outputDir)
	}()

	wg.Wait()
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