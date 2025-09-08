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
func CalculateECommerceDSRowCounts(targetGB float64) (ECommerceDSRowCounts, error) {
	if targetGB <= 0 {
		return ECommerceDSRowCounts{}, fmt.Errorf("target size must be positive")
	}

	targetBytes := targetGB * 1024 * 1024 * 1024

	// Average effective size of a fact row, assuming a mix of store, web, and catalog sales.
	const effectiveSizePerFactRow = 150.0 // Adjusted based on the mix of fact tables

	// The relative weights of each fact table. Ratios are approximately 2:1:2 for store:catalog:web.
	const totalFactRatio = 2.0 + 1.0 + 2.0 // Store + Catalog + Web

	totalFactRows := int(math.Max(1.0, math.Round(targetBytes/effectiveSizePerFactRow)))

	// Distribute total rows according to the defined ratios
	numStoreSales := int(float64(totalFactRows) * (2.0 / totalFactRatio))
	numCatalogSales := int(float64(totalFactRows) * (1.0 / totalFactRatio))
	numWebSales := int(float64(totalFactRows) * (2.0 / totalFactRatio))

	// --- Derive Dimension Counts from Fact Counts for Realism ---
	numCustomers := int(math.Max(100.0, float64(totalFactRows)/25.0))      // Avg 25 sales events per customer
	numItems := int(math.Max(100.0, float64(totalFactRows)/100.0))        // Avg 100 sales per item
	numCustomerAddresses := int(float64(numCustomers) * 1.5)
	numPromotions := int(math.Max(20.0, float64(numItems)/10.0))
	numWebPages := int(math.Max(100.0, float64(numCustomers)/2.0)) // Half of customers have web activity

	counts := ECommerceDSRowCounts{
		StoreSales:   numStoreSales,
		CatalogSales: numCatalogSales,
		WebSales:     numWebSales,

		// Scaled Dimensions
		Customers:             numCustomers,
		CustomerAddresses:     numCustomerAddresses,
		Items:                 numItems,
		Promotions:            numPromotions,
		WebPages:              numWebPages,

		// Fixed-size or slowly growing dimensions
		CustomerDemographics:  1000,
		HouseholdDemographics: 1000,
		Stores:                10,
		CallCenters:           5,
		CatalogPages:          100,
		WebSites:              10,
		Warehouses:            10,
		Reasons:               20,
		ShipModes:             5,
		IncomeBands:           10,

		// Derived Counts
		StoreReturns:   numStoreSales / 10,
		CatalogReturns: numCatalogSales / 10,
		WebReturns:     numWebSales / 10,
		Inventory:      numItems * 5, // Inventory for 5x the number of items
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
		return 53, nil // Reduced by ~10 bytes after removing total_price column
	// Financial (empirical estimates based on CSV output from calibration run)
	case "company":
		return 78, nil
	case "exchange":
		return 37, nil
	case "daily_stock_price":
		return 120, nil
	// Medical (placeholder estimates)
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

	// Empirical effective size per order item (includes proportional share of all dimensions and header)
	const effectiveSizePerOrderItem = 65.0 // Bytes/item, recalibrated after removing total_price column

	numOrderItems := int(math.Max(1.0, math.Round(targetBytes/effectiveSizePerOrderItem)))

	// Derive other counts based on numOrderItems and original ratios
	numOrderHeaders := int(math.Max(1.0, math.Round(float64(numOrderItems)/AvgItemsPerOrder)))
	numCustomers := int(math.Max(1.0, math.Round(float64(numOrderHeaders)/DefaultOrdersPerCustomerRatio)))
	numCustomerAddresses := int(math.Max(1.0, math.Round(float64(numCustomers)*AvgAddressesPerCustomer)))

	// New logic: Derive suppliers from customers, and products from suppliers.
	// This creates a more realistic hierarchy and decouples products from orders.
	numSuppliers := int(math.Max(1.0, math.Round(float64(numCustomers)/100.0))) // Assume 100 customers per supplier
	numProducts := int(math.Max(1.0, math.Round(float64(numSuppliers)*DefaultProductsPerSupplierRatio)))

	// Ensure minimums if calculations result in zero
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

	// Empirical effective size per daily stock price (includes proportional share of all dimensions)
	const effectiveSizePerDailyStockPrice = 139.0 // Bytes/price, derived from calibration

	numPrices := int(math.Max(1.0, math.Round(targetBytes/effectiveSizePerDailyStockPrice)))

	// Derive other counts based on numPrices and original ratios
	numCompanies := int(math.Max(1.0, math.Round(float64(numPrices)/(AvgTradingDaysPerYear*NumYearsOfData))))
	numExchanges := 5 // Fixed number of exchanges

	// Ensure minimums
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

	// Placeholder logic for sizing the medical model
	const effectiveSizePerAppointment = 200.0 // Bytes/appointment, placeholder

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
