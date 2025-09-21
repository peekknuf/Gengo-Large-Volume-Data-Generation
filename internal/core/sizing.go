package core

import (
	"fmt"
	"math"

	financialsimulation "github.com/peekknuf/Gengo/internal/simulation/financial"
	medicalsimulation "github.com/peekknuf/Gengo/internal/simulation/medical"
)

const (
	AvgItemsPerOrder              = 5.0
	DefaultOrdersPerCustomerRatio = 10.0
	DefaultProductsPerSupplierRatio = 25.0
	AvgAddressesPerCustomer       = 1.5
)

const (
	AvgTradingDaysPerYear = 252
	NumYearsOfData        = 5
)

const (
	AvgBytesStringTiny   = 8
	AvgBytesStringShort  = 15
	AvgBytesStringMedium = 30
	AvgBytesStringLong   = 50
	SizeBytesInt         = 4
	SizeBytesInt64       = 8
	SizeBytesFloat64     = 8
	SizeBytesTimestamp   = 8
)

type ECommerceRowCounts struct {
	Customers         int
	CustomerAddresses int
	Suppliers         int
	Products          int
	OrderHeaders      int
	OrderItems        int
	ProductCategories int
}

// CalculateECommerceDSRowCounts determines the target number of rows for each e-commerce-ds table.
// This enhanced version uses realistic TPC-DS sizing based on business patterns and accurate row size estimates.
func CalculateECommerceDSRowCounts(targetGB float64) (ECommerceDSRowCounts, error) {
	if targetGB <= 0 {
		return ECommerceDSRowCounts{}, fmt.Errorf("target size must be positive")
	}

	targetBytes := targetGB * 1024 * 1024 * 1024

	// Realistic row size estimates based on TPC-DS schema analysis
	const (
		storeSalesRowSize     = 245.0 // Complete TPC-DS store_sales with all fields
		storeReturnsRowSize   = 195.0 // Complete TPC-DS store_returns with all fields
		catalogSalesRowSize   = 305.0 // Complete TPC-DS catalog_sales with all fields
		catalogReturnsRowSize = 255.0 // Complete TPC-DS catalog_returns with all fields
		webSalesRowSize       = 285.0 // Complete TPC-DS web_sales with all fields
		webReturnsRowSize     = 215.0 // Complete TPC-DS web_returns with all fields
		inventoryRowSize      = 48.0  // Complete TPC-DS inventory with all fields
	)

	// Business-realistic sales channel distribution (modern e-commerce patterns)
	const (
		storeSalesRatio   = 0.55 // 55% traditional retail
		webSalesRatio     = 0.30 // 30% e-commerce
		catalogSalesRatio = 0.15 // 15% catalog sales
	)

	// Return rates by channel (industry averages)
	const (
		storeReturnRate   = 0.08 // 8% for in-store purchases
		webReturnRate     = 0.12 // 12% for online purchases
		catalogReturnRate = 0.10 // 10% for catalog purchases
	)

	// Calculate base fact table distribution using weighted average row size
	avgFactRowSize := storeSalesRowSize*storeSalesRatio +
					  webSalesRowSize*webSalesRatio +
					  catalogSalesRowSize*catalogSalesRatio

	// Adjust divisor for complete TPC-DS schema (17 dimensions + 7 fact tables)
	// Reduced from 0.9 to 0.7 to achieve target 1GB (was generating 793MB)
	totalSalesRows := int(math.Max(1.0, math.Round(targetBytes / avgFactRowSize / 0.7)))

	numStoreSales := int(float64(totalSalesRows) * storeSalesRatio)
	numWebSales := int(float64(totalSalesRows) * webSalesRatio)
	numCatalogSales := int(float64(totalSalesRows) * catalogSalesRatio)

	numStoreReturns := int(float64(numStoreSales) * storeReturnRate)
	numWebReturns := int(float64(numWebSales) * webReturnRate)
	numCatalogReturns := int(float64(numCatalogSales) * catalogReturnRate)

	// Business-driven dimension scaling for complete TPC-DS schema
	numCustomers := int(math.Max(1000.0, float64(numStoreSales+numWebSales+numCatalogSales) / 25.0)) // 25 transactions per customer annually
	numItems := int(math.Max(2000.0, float64(numStoreSales+numWebSales+numCatalogSales) / 120.0)) // 120 sales per item annually
	numCustomerAddresses := int(float64(numCustomers) * 2.2) // Average 2.2 addresses per customer
	numPromotions := int(math.Max(150.0, float64(numItems) / 6.0)) // Average 6 items per promotion
	numWebPages := int(math.Max(8000.0, float64(numCustomers) / 2.5)) // Average 2.5 customers per web page

	// Scale fixed dimensions based on business size
	businessScaleFactor := math.Sqrt(float64(numCustomers) / 100000.0) // Normalize to 100K customer base

	stores := int(math.Max(8.0, 15.0 * businessScaleFactor))
	warehouses := int(math.Max(5.0, 12.0 * businessScaleFactor))
	callCenters := int(math.Max(3.0, 8.0 * businessScaleFactor))
	webSites := int(math.Max(3.0, 10.0 * businessScaleFactor))
	catalogPages := int(math.Max(80.0, 200.0 * businessScaleFactor))

	// Customer and household demographics scale with customer base
	customerDemographics := int(math.Max(800.0, float64(numCustomers) * 0.025)) // 2.5% of customers
	householdDemographics := int(math.Max(500.0, float64(numCustomers) * 0.018)) // 1.8% of customers
	incomeBands := int(math.Max(10.0, 15.0 * businessScaleFactor))
	reasons := int(math.Max(20.0, 35.0 * businessScaleFactor))
	shipModes := int(math.Max(6.0, 10.0 * businessScaleFactor))

	// Inventory: track weekly inventory for all items across warehouses
	inventoryWeeks := 52 // One year of weekly snapshots
	numInventory := numItems * warehouses * inventoryWeeks / 4 // Quarterly snapshots

	counts := ECommerceDSRowCounts{
		StoreSales:     numStoreSales,
		WebSales:       numWebSales,
		CatalogSales:   numCatalogSales,
		StoreReturns:   numStoreReturns,
		WebReturns:     numWebReturns,
		CatalogReturns: numCatalogReturns,
		Inventory:      numInventory,
		Customers:             numCustomers,
		Items:                 numItems,
		CustomerAddresses:     numCustomerAddresses,
		Promotions:            numPromotions,
		WebPages:              numWebPages,
		CustomerDemographics:  customerDemographics,
		HouseholdDemographics: householdDemographics,
		Stores:                stores,
		CallCenters:           callCenters,
		CatalogPages:          catalogPages,
		WebSites:              webSites,
		Warehouses:            warehouses,
		Reasons:               reasons,
		ShipModes:             shipModes,
		IncomeBands:           incomeBands,
	}

	return counts, nil
}

// estimateAvgRowSizeBytes estimates the average uncompressed size of a single row for a given table type.
func estimateAvgRowSizeBytes(tableType string) (int, error) {
	switch tableType {
	// E-commerce (empirical estimates based on CSV output from calibration run)
	case "customer":
		return 78, nil
	case "customer_address":
		return 136, nil
	case "supplier":
		return 66, nil
	case "product":
		return 84, nil
	case "order_header":
		return 111, nil
	case "order_item":
		return 53, nil // Optimized after removing total_price column
	// Financial (empirical estimates based on CSV output from calibration run)
	case "company":
		return 78, nil
	case "exchange":
		return 37, nil
	case "daily_stock_price":
		return 120, nil
	case "patient":
		return 80, nil
	case "doctor":
		return 70, nil
	case "clinic":
		return 100, nil
	case "appointment":
		return 90, nil
	default:
		return 0, fmt.Errorf("unknown table type: %s", tableType)
	}
}

// CalculateECommerceRowCounts determines the target number of rows for each e-commerce table.
// It aims to distribute the target size across all tables.
func CalculateECommerceRowCounts(targetGB float64) (ECommerceRowCounts, error) {
	if targetGB <= 0 {
		return ECommerceRowCounts{}, fmt.Errorf("target size must be positive")
	}

	targetBytes := targetGB * 1024 * 1024 * 1024

	const effectiveSizePerOrderItem = 65.0 // Bytes/item, includes proportional share of all dimensions and header

	numOrderItems := int(math.Max(1.0, math.Round(targetBytes/effectiveSizePerOrderItem)))

	numOrderHeaders := int(math.Max(1.0, math.Round(float64(numOrderItems)/AvgItemsPerOrder)))
	numCustomers := int(math.Max(1.0, math.Round(float64(numOrderHeaders)/DefaultOrdersPerCustomerRatio)))
	numCustomerAddresses := int(math.Max(1.0, math.Round(float64(numCustomers)*AvgAddressesPerCustomer)))

	// Create realistic business hierarchy: suppliers → products → customers
	numSuppliers := int(math.Max(1.0, math.Round(float64(numCustomers)/100.0))) // 100 customers per supplier
	numProducts := int(math.Max(1.0, math.Round(float64(numSuppliers)*DefaultProductsPerSupplierRatio)))

	// Ensure minimum values for edge cases
	if numOrderItems == 0 && targetGB > 0 {
		numOrderItems = 1
		numOrderHeaders = 1
		numCustomers = 1
		numCustomerAddresses = 1
		numProducts = 1
		numSuppliers = 1
	}

	counts := ECommerceRowCounts{
		Customers:         numCustomers,
		CustomerAddresses: numCustomerAddresses,
		Suppliers:         numSuppliers,
		Products:          numProducts,
		OrderHeaders:      numOrderHeaders,
		OrderItems:        numOrderItems,
		ProductCategories: 10,
	}

	return counts, nil
}

// CalculateFinancialRowCounts determines the target number of rows for each financial table.
// It aims to distribute the target size across all tables, accounting for fixed dimensions.
// Note: Achieving an exact target size is challenging due to:
// 1. Inaccurate `estimateAvgRowSizeBytes` (estimates are averages, actual CSV sizes vary).
// 2. Filesystem overhead (du -sh reports disk usage, not just raw data size).
// 3. Integer rounding in row count calculations.
// 4. Fixed dimensions (like exchanges) can impact accuracy for very small/large targets.
func CalculateFinancialRowCounts(targetGB float64) (financialsimulation.FinancialRowCounts, error) {
	if targetGB <= 0 {
		return financialsimulation.FinancialRowCounts{}, fmt.Errorf("target size must be positive")
	}

	targetBytes := targetGB * 1024 * 1024 * 1024

	const effectiveSizePerDailyStockPrice = 139.0 // Bytes/price, includes proportional share of all dimensions

	numPrices := int(math.Max(1.0, math.Round(targetBytes/effectiveSizePerDailyStockPrice)))

	numCompanies := int(math.Max(1.0, math.Round(float64(numPrices)/(AvgTradingDaysPerYear*NumYearsOfData))))
	numExchanges := 5 // Fixed number of exchanges

	// Ensure minimum values for edge cases
	if numCompanies == 0 && numPrices > 0 {
		numCompanies = 1
	} else if numCompanies == 0 && numPrices == 0 {
		numCompanies = 1
		numPrices = AvgTradingDaysPerYear * NumYearsOfData
	}

	counts := financialsimulation.FinancialRowCounts{
		Companies:        numCompanies,
		Exchanges:        numExchanges,
		DailyStockPrices: numPrices,
	}

	return counts, nil
}

// CalculateMedicalRowCounts determines the target number of rows for each medical table.
func CalculateMedicalRowCounts(targetGB float64) (medicalsimulation.MedicalRowCounts, error) {
	if targetGB <= 0 {
		return medicalsimulation.MedicalRowCounts{}, fmt.Errorf("target size must be positive")
	}

	targetBytes := targetGB * 1024 * 1024 * 1024

	const effectiveSizePerAppointment = 200.0 // Bytes/appointment, estimate for medical data

	numAppointments := int(math.Max(1.0, math.Round(targetBytes/effectiveSizePerAppointment)))
	numPatients := int(math.Max(1.0, math.Round(float64(numAppointments)/5.0))) // 5 appointments per patient
	numDoctors := int(math.Max(1.0, math.Round(float64(numPatients)/100.0)))   // 100 patients per doctor
	numClinics := int(math.Max(1.0, math.Round(float64(numDoctors)/20.0)))      // 20 doctors per clinic

	counts := medicalsimulation.MedicalRowCounts{
		Patients:     numPatients,
		Doctors:      numDoctors,
		Clinics:      numClinics,
		Appointments: numAppointments,
	}

	return counts, nil
}
