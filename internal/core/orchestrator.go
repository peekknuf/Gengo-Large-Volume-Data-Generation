package core

import (
	"fmt"
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

	householdDemographics = ecommercedssimulation.GenerateHouseholdDemographics(counts.HouseholdDemographics, getSKsFromSlice(incomeBands))
	promotions = ecommercedssimulation.GeneratePromotions(counts.Promotions, getSKsFromSlice(items))
	customers = ecommercedssimulation.GenerateCustomers(counts.Customers, getSKsFromSlice(customerDemographics), getSKsFromSlice(householdDemographics), getSKsFromSlice(customerAddresses))

	var writersWg sync.WaitGroup
	writersWg.Add(17) // All dimension writers

	go func() {
		defer writersWg.Done()
		errChan <- formats.WriteSliceData(items, "dim_items", format, outputDir)
	}()
	go func() {
		defer writersWg.Done()
		errChan <- formats.WriteSliceData(customers, "dim_customers", format, outputDir)
	}()
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
	go func() {
		defer writersWg.Done()
		errChan <- formats.WriteSliceData(promotions, "dim_promotions", format, outputDir)
	}()
	go func() {
		defer writersWg.Done()
		errChan <- formats.WriteSliceData(stores, "dim_stores", format, outputDir)
	}()
	go func() {
		defer writersWg.Done()
		errChan <- formats.WriteSliceData(callCenters, "dim_call_centers", format, outputDir)
	}()
	go func() {
		defer writersWg.Done()
		errChan <- formats.WriteSliceData(catalogPages, "dim_catalog_pages", format, outputDir)
	}()
	go func() {
		defer writersWg.Done()
		errChan <- formats.WriteSliceData(webSites, "dim_web_sites", format, outputDir)
	}()
	go func() {
		defer writersWg.Done()
		errChan <- formats.WriteSliceData(webPages, "dim_web_pages", format, outputDir)
	}()
	go func() {
		defer writersWg.Done()
		errChan <- formats.WriteSliceData(warehouses, "dim_warehouses", format, outputDir)
	}()
	go func() {
		defer writersWg.Done()
		errChan <- formats.WriteSliceData(reasons, "dim_reasons", format, outputDir)
	}()
	go func() {
		defer writersWg.Done()
		errChan <- formats.WriteSliceData(shipModes, "dim_ship_modes", format, outputDir)
	}()
	go func() {
		defer writersWg.Done()
		errChan <- formats.WriteSliceData(incomeBands, "dim_income_bands", format, outputDir)
	}()
	go func() {
		defer writersWg.Done()
		errChan <- formats.WriteSliceData(timeDim, "dim_time", format, outputDir)
	}()
	go func() {
		defer writersWg.Done()
		errChan <- formats.WriteSliceData(dateDim, "dim_date", format, outputDir)
	}()

	dimSKs := map[string][]int64{
		"items":                  getSKsFromSlice(items),
		"customers":              getSKsFromSlice(customers),
		"customer_addresses":     getSKsFromSlice(customerAddresses),
		"customer_demographics":  getSKsFromSlice(customerDemographics),
		"household_demographics": getSKsFromSlice(householdDemographics),
		"promotions":             getSKsFromSlice(promotions),
		"stores":                 getSKsFromSlice(stores),
		"call_centers":           getSKsFromSlice(callCenters),
		"catalog_pages":          getSKsFromSlice(catalogPages),
		"web_sites":              getSKsFromSlice(webSites),
		"web_pages":              getSKsFromSlice(webPages),
		"warehouses":             getSKsFromSlice(warehouses),
		"ship_modes":             getSKsFromSlice(shipModes),
	}

	writersWg.Add(3) // One for each fact table generator
	go func() {
		defer writersWg.Done()
		if err := ecommercedssimulation.GenerateStoreSalesOptimized(counts.StoreSales, dimSKs["items"], dimSKs["customers"], dimSKs["stores"], dimSKs["promotions"], outputDir); err != nil {
			errChan <- fmt.Errorf("failed to generate store sales: %w", err)
		}
	}()
	go func() {
		defer writersWg.Done()
		if err := ecommercedssimulation.GenerateCatalogSalesOptimized(counts.CatalogSales, dimSKs["items"], dimSKs["customers"], dimSKs["customer_demographics"], dimSKs["household_demographics"], dimSKs["customer_addresses"], dimSKs["call_centers"], dimSKs["catalog_pages"], dimSKs["ship_modes"], dimSKs["warehouses"], dimSKs["promotions"], outputDir); err != nil {
			errChan <- fmt.Errorf("failed to generate catalog sales: %w", err)
		}
	}()
	go func() {
		defer writersWg.Done()
		if err := ecommercedssimulation.GenerateWebSalesOptimized(counts.WebSales, dimSKs["items"], dimSKs["customers"], dimSKs["customer_demographics"], dimSKs["household_demographics"], dimSKs["customer_addresses"], dimSKs["web_pages"], dimSKs["web_sites"], dimSKs["ship_modes"], dimSKs["warehouses"], dimSKs["promotions"], outputDir); err != nil {
			errChan <- fmt.Errorf("failed to generate web sales: %w", err)
		}
	}()

	writersWg.Wait()
	close(errChan)

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

	var customers []ecommercemodels.Customer
	var suppliers []ecommercemodels.Supplier
	var customerAddresses []ecommercemodels.CustomerAddress
	var products []ecommercemodels.Product
	var productCategories []ecommercemodels.ProductCategory

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

	writersWg.Add(5)
	go func() {
		defer writersWg.Done()
		customersWg.Wait()
		if err := formats.WriteCustomers(customers, outputDir, format); err != nil {
			errChan <- err
		}
	}()
	go func() {
		defer writersWg.Done()
		customersWg.Wait()
		if err := formats.WriteCustomerAddresses(customerAddresses, outputDir, format); err != nil {
			errChan <- err
		}
	}()
	go func() {
		defer writersWg.Done()
		suppliersWg.Wait()
		if err := formats.WriteSuppliers(suppliers, outputDir, format); err != nil {
			errChan <- err
		}
	}()
	go func() {
		defer writersWg.Done()
		categoriesWg.Wait()
		if err := formats.WriteProductCategories(productCategories, outputDir, format); err != nil {
			errChan <- err
		}
	}()
	go func() {
		defer writersWg.Done()
		productsWg.Wait()
		if err := formats.WriteProducts(products, outputDir, format); err != nil {
			errChan <- err
		}
	}()

	writersWg.Add(1) // Add fact generation to the wait group
	go func() {
		defer writersWg.Done() // Mark fact generation as done
		// Wait for customer and product data to be ready
		customersWg.Wait()
		productsWg.Wait()

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

		if err := ecommerce.GenerateECommerceModelData(counts.OrderHeaders, customerIDs, customerAddresses, productDetails, productIDsForSampling, outputDir, format); err != nil {
			errChan <- err
		}
	}()

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

	wg.Add(2)
	go func() {
		defer wg.Done()
		errChan <- formats.WriteSliceData(companies, "dim_companies", format, outputDir)
	}()
	go func() {
		defer wg.Done()
		errChan <- formats.WriteSliceData(exchanges, "dim_exchanges", format, outputDir)
	}()

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
