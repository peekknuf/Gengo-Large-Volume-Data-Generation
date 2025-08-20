package core

import (
	"fmt"
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

	// --- Concurrent Dimension Generation ---
	var customers []ecommercemodels.Customer
	var suppliers []ecommercemodels.Supplier
	var customerAddresses []ecommercemodels.CustomerAddress
	var products []ecommercemodels.Product
	var productCategories []ecommercemodels.ProductCategory

	var genWg sync.WaitGroup
	genWg.Add(3)

	go func() {
		defer genWg.Done()
		customers = ecommerce.GenerateCustomers(counts.Customers)
		customerAddresses = ecommerce.GenerateCustomerAddresses(customers)
	}()

	go func() {
		defer genWg.Done()
		suppliers = ecommerce.GenerateSuppliers(counts.Suppliers)
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

	go func() {
		defer genWg.Done()
		productCategories = ecommerce.GenerateProductCategories()
	}()

	genWg.Wait() // Wait for all dimension data to be generated

	// --- Concurrent Dimension Writing ---
	wg.Add(5)
	go func() {
		defer wg.Done()
		errChan <- formats.WriteSliceData(customers, "dim_customers", format, outputDir)
	}()
	go func() {
		defer wg.Done()
		errChan <- formats.WriteSliceData(customerAddresses, "dim_customer_addresses", format, outputDir)
	}()
	go func() {
		defer wg.Done()
		errChan <- formats.WriteSliceData(suppliers, "dim_suppliers", format, outputDir)
	}()
	go func() {
		defer wg.Done()
		errChan <- formats.WriteSliceData(products, "dim_products", format, outputDir)
	}()
	go func() {
		defer wg.Done()
		errChan <- formats.WriteSliceData(productCategories, "dim_product_categories", format, outputDir)
	}()

	// --- Fact Table Generation (can run while dimensions are writing) ---
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

	headers, items, err := ecommerce.GenerateECommerceModelData(counts.OrderHeaders, customerIDs, customerAddresses, productInfo, productIDsForSampling)
	if err != nil {
		return err // This error is critical and should stop the process
	}

	// --- Concurrent Fact Table Writing ---
	wg.Add(2)
	go func() {
		defer wg.Done()
		errChan <- formats.WriteSliceData(headers, "fact_orders_header", format, outputDir)
	}()
	go func() {
		defer wg.Done()
		errChan <- formats.WriteSliceData(items, "fact_order_items", format, outputDir)
	}()

	wg.Wait()
	close(errChan)

	for err := range errChan {
		if err != nil {
			// Return the first error encountered
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