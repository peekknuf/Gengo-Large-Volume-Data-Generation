// cmd/sizing.go
package core

import (
	"fmt"
	"math"

	financialsimulation "github.com/peekknuf/Gengo/internal/simulation/financial"
	medicalsimulation "github.com/peekknuf/Gengo/internal/simulation/medical"
)

// --- E-commerce Configuration ---
const (
	AvgItemsPerOrder              = 5.0
	DefaultOrdersPerCustomerRatio = 10.0
	DefaultProductsPerSupplierRatio = 25.0
	AvgAddressesPerCustomer       = 1.5
)

// --- Financial Configuration ---
const (
	AvgTradingDaysPerYear = 252
	NumYearsOfData        = 5
)

// Estimated Average Size in Bytes for Different Field Types (Uncompressed)
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

// ECommerceRowCounts holds the calculated number of rows for each e-commerce table
type ECommerceRowCounts struct {
	Customers         int
	CustomerAddresses int
	Suppliers         int
	Products          int
	OrderHeaders      int
	OrderItems        int
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
		return 63, nil
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
	const effectiveSizePerOrderItem = 148.0 // Bytes/item, derived from calibration

	numOrderItems := int(math.Max(1.0, math.Round(targetBytes/effectiveSizePerOrderItem)))

	// Derive other counts based on numOrderItems and original ratios
	numOrderHeaders := int(math.Max(1.0, math.Round(float64(numOrderItems)/AvgItemsPerOrder)))
	numCustomers := int(math.Max(1.0, math.Round(float64(numOrderHeaders)/DefaultOrdersPerCustomerRatio)))
	numCustomerAddresses := int(math.Max(1.0, math.Round(float64(numCustomers)*AvgAddressesPerCustomer)))
	numProducts := int(math.Max(1.0, math.Round(float64(numOrderItems)/AvgItemsPerOrder)))
	numSuppliers := int(math.Max(1.0, math.Round(float64(numProducts)/DefaultProductsPerSupplierRatio)))

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
		Customers:        numCustomers,
		CustomerAddresses: numCustomerAddresses,
		Suppliers:        numSuppliers,
		Products:         numProducts,
		OrderHeaders:     numOrderHeaders,
		OrderItems:       numOrderItems,
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
