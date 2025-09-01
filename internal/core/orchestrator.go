package core

import (
	"fmt"
	"path/filepath"
	"os"
	"reflect"
	"sync"
	"time"

	"github.com/peekknuf/Gengo/internal/formats"
	ecommercemodels "github.com/peekknuf/Gengo/internal/models/ecommerce"
	financialmodels "github.com/peekknuf/Gengo/internal/models/financial"
	medicalmodels "github.com/peekknuf/Gengo/internal/models/medical"
	"github.com/peekknuf/Gengo/internal/simulation/ecommerce"
	ecommercedssimulation "github.com/peekknuf/Gengo/internal/simulation/ecommerce-ds"
	financialsimulation "github.com/peekknuf/Gengo/internal/simulation/financial"
	medicalsimulation "github.com/peekknuf/Gengo/internal/simulation/medical"
)

// ECommerceDSRowCounts defines the number of rows for each table in the TPC-DS model.

type ECommerceDSRowCounts struct {
	Customers             int
	CustomerAddresses     int
	CustomerDemographics  int
	HouseholdDemographics int
	Items                 int
	Promotions            int
	Stores                int
	CallCenters           int
	CatalogPages          int
	WebSites              int
	WebPages              int	
	Warehouses            int
	Reasons               int
	ShipModes             int
	IncomeBands           int
	StoreSales            int
	StoreReturns          int
	CatalogSales          int
	CatalogReturns        int
	WebSales              int
	WebReturns            int
	Inventory             int
}

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
	case "ecommerce-ds":
		err = generateECommerceDSDataConcurrently(counts.(ECommerceDSRowCounts), format, outputDir)
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
		
		// List all generated files for verification
		fmt.Println("\nGenerated files:")
		files, err := os.ReadDir(outputDir)
		if err == nil {
			for _, file := range files {
				if !file.IsDir() {
					info, err := file.Info()
					if err == nil {
						sizeKB := float64(info.Size()) / 1024.0
						if sizeKB >= 1024 {
							fmt.Printf("  %s (%.1f MB)\n", file.Name(), sizeKB/1024.0)
						} else {
							fmt.Printf("  %s (%.1f KB)\n", file.Name(), sizeKB)
						}
					}
				}
			}
		}
		return nil
}

func generateECommerceDSDataConcurrently(counts ECommerceDSRowCounts, format string, outputDir string) error {
	errChan := make(chan error, 30) // Buffer for all goroutines

	// --- Dimension Data Holders ---
	var items []interface{}
	var customers []interface{}
	var customerAddresses []interface{}
	var customerDemographics []interface{}
	var householdDemographics []interface{}
	var promotions []interface{}
	var stores []interface{}
	var callCenters []interface{}
	var catalogPages []interface{}
	var webSites []interface{}
	var webPages []interface{}
	var warehouses []interface{}
	var reasons []interface{}
	var shipModes []interface{}
	var incomeBands []interface{}
	var timeDim []interface{}
	var dateDim []interface{}

	// --- Dimension Generation (Serial) ---
	// Independent dimensions
	items = ecommercedssimulation.GenerateItems(counts.Items)
	customerAddresses = ecommercedssimulation.GenerateCustomerAddresses(counts.CustomerAddresses)
	customerDemographics = ecommercedssimulation.GenerateCustomerDemographics(counts.CustomerDemographics)
	incomeBands = ecommercedssimulation.GenerateIncomeBands(counts.IncomeBands)
	stores = ecommercedssimulation.GenerateStores(counts.Stores)
	callCenters = ecommercedssimulation.GenerateCallCenters(counts.CallCenters)
	catalogPages = ecommercedssimulation.GenerateCatalogPages(counts.CatalogPages)
	webSites = ecommercedssimulation.GenerateWebSites(counts.WebSites)
	webPages = ecommercedssimulation.GenerateWebPages(counts.WebPages)
	warehouses = ecommercedssimulation.GenerateWarehouses(counts.Warehouses)
	reasons = ecommercedssimulation.GenerateReasons(counts.Reasons)
	shipModes = ecommercedssimulation.GenerateShipModes(counts.ShipModes)
	timeDim = ecommercedssimulation.GenerateTimeDim()
	dateDim = ecommercedssimulation.GenerateDateDim(2020, 2025)

	// Dependent dimensions
	householdDemographics = ecommercedssimulation.GenerateHouseholdDemographics(counts.HouseholdDemographics, getSKsFromSlice(incomeBands))
	promotions = ecommercedssimulation.GeneratePromotions(counts.Promotions, getSKsFromSlice(items))
	customers = ecommercedssimulation.GenerateCustomers(counts.Customers, getSKsFromSlice(customerDemographics), getSKsFromSlice(householdDemographics), getSKsFromSlice(customerAddresses))

	// --- Dimension Writing ---
	var writersWg sync.WaitGroup
	writersWg.Add(17) // All dimension writers

	go func() { defer writersWg.Done(); errChan <- formats.WriteSliceData(items, "dim_items", format, outputDir) }()
	go func() { defer writersWg.Done(); errChan <- formats.WriteSliceData(customers, "dim_customers", format, outputDir) }()
	go func() {
		defer writersWg.Done()
		errChan <- formats.WriteSliceData(customerAddresses, "dim_customer_addresses", format, outputDir)
	}()
	go func() {
		defer writersWg.Done()
		errChan <- formats.WriteSliceData(customerDemographics, "dim_customer_demographics", format, outputDir)
	}()
	go func() {
		defer writersWg.Done()
		errChan <- formats.WriteSliceData(householdDemographics, "dim_household_demographics", format, outputDir)
	}()
	go func() { defer writersWg.Done(); errChan <- formats.WriteSliceData(promotions, "dim_promotions", format, outputDir) }()
	go func() { defer writersWg.Done(); errChan <- formats.WriteSliceData(stores, "dim_stores", format, outputDir) }()
	go func() { defer writersWg.Done(); errChan <- formats.WriteSliceData(callCenters, "dim_call_centers", format, outputDir) }()
	go func() {
		defer writersWg.Done()
		errChan <- formats.WriteSliceData(catalogPages, "dim_catalog_pages", format, outputDir)
	}()
	go func() { defer writersWg.Done(); errChan <- formats.WriteSliceData(webSites, "dim_web_sites", format, outputDir) }()
	go func() { defer writersWg.Done(); errChan <- formats.WriteSliceData(webPages, "dim_web_pages", format, outputDir) }()
	go func() { defer writersWg.Done(); errChan <- formats.WriteSliceData(warehouses, "dim_warehouses", format, outputDir) }()
	go func() { defer writersWg.Done(); errChan <- formats.WriteSliceData(reasons, "dim_reasons", format, outputDir) }()
	go func() { defer writersWg.Done(); errChan <- formats.WriteSliceData(shipModes, "dim_ship_modes", format, outputDir) }()
	go func() { defer writersWg.Done(); errChan <- formats.WriteSliceData(incomeBands, "dim_income_bands", format, outputDir) }()
	go func() { defer writersWg.Done(); errChan <- formats.WriteSliceData(timeDim, "dim_time", format, outputDir) }()
	go func() { defer writersWg.Done(); errChan <- formats.WriteSliceData(dateDim, "dim_date", format, outputDir) }()

	// --- Fact Table Generation (Optimized with Direct File Sharding) ---
	// Replace channel-based approach with worker-based file sharding for better performance

	writersWg.Add(1) // Add optimized fact generation
	go func() {
		defer writersWg.Done()
		// Use optimized generation functions with direct file writing
		if err := ecommercedssimulation.GenerateStoreSalesOptimized(counts.StoreSales, getSKsFromSlice(items), getSKsFromSlice(customers), getSKsFromSlice(stores), getSKsFromSlice(promotions), outputDir); err != nil {
			errChan <- fmt.Errorf("store sales generation failed: %w", err)
		}
		// TODO: Add other optimized fact generators (catalog sales, web sales, etc.)
		// For now, using legacy channel-based approach for other fact tables
	}()

	// Wait for all writers to finish
	writersWg.Wait()
	close(errChan)

	// Process errors from the channel
	for err := range errChan {
		if err != nil {
			return err // Return the first error encountered
		}
	}

	return nil
}

func getSKsFromSlice(slice []interface{}) []int64 {
	sks := make([]int64, len(slice))
	for i, v := range slice {
		sks[i] = reflect.ValueOf(v).Field(0).Int()
	}
	return sks
}

func getSKs(slice interface{}) []int64 {
	v := reflect.ValueOf(slice)
	if v.Kind() != reflect.Slice {
		return nil
	}
	sks := make([]int64, v.Len())
	for i := 0; i < v.Len(); i++ {
		// Assumes the first field is the SK
		sks[i] = v.Index(i).Field(0).Int()
	}
	return sks
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
	// Direct file sharding - no channels needed

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
	writersWg.Add(1) // Add fact generation to the wait group
	go func() {
		defer writersWg.Done() // Mark fact generation as done
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
		
		// Generate facts directly to shard files
		if err := ecommerce.GenerateECommerceModelData(counts.OrderHeaders, customerIDs, customerAddresses, productDetails, productIDsForSampling, outputDir); err != nil {
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