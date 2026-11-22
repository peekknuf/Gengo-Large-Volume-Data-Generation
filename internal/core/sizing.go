package core

import (
	"fmt"
	"math"

	financialsimulation "github.com/peekknuf/Gengo/internal/simulation/financial"
	medicalsimulation "github.com/peekknuf/Gengo/internal/simulation/medical"
)

const (
	AvgItemsPerOrder                = 5.5
	DefaultOrdersPerCustomerRatio   = 10.0
	DefaultProductsPerSupplierRatio = 25.0
	AvgAddressesPerCustomer         = 1.5
)

const (
	AvgTradingDaysPerYear = 252
	NumYearsOfData        = 5
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
func CalculateECommerceDSRowCounts(targetGB float64, format string) (ECommerceDSRowCounts, error) {
	if targetGB <= 0 {
		return ECommerceDSRowCounts{}, fmt.Errorf("target size must be positive")
	}

	targetBytes := targetGB * 1024 * 1024 * 1024

	const (
		storeSalesRowSize = 245.0

		catalogSalesRowSize = 305.0

		webSalesRowSize = 285.0
	)

	const (
		storeSalesRatio   = 0.55
		webSalesRatio     = 0.30
		catalogSalesRatio = 0.15
	)

	avgFactRowSize := storeSalesRowSize*storeSalesRatio +
		webSalesRowSize*webSalesRatio +
		catalogSalesRowSize*catalogSalesRatio

	formatMultiplier := 1.0
	if format == "parquet" {
		formatMultiplier = 1.5
	}

	rows := math.Max(1, math.Round(targetBytes/avgFactRowSize/0.7*formatMultiplier))
	if math.IsNaN(rows) || rows >= float64(math.MaxInt) {
		return ECommerceDSRowCounts{}, fmt.Errorf("target size exceeds supported row count")
	}
	totalSalesRows := int(rows)

	numWebSales := int(float64(totalSalesRows) * webSalesRatio)
	numCatalogSales := int(float64(totalSalesRows) * catalogSalesRatio)
	numStoreSales := totalSalesRows - numWebSales - numCatalogSales

	numCustomers := int(math.Max(1000.0, float64(numStoreSales+numWebSales+numCatalogSales)/25.0))
	numItems := int(math.Max(2000.0, float64(numStoreSales+numWebSales+numCatalogSales)/120.0))
	numCustomerAddresses := int(float64(numCustomers) * 2.2)
	numPromotions := int(math.Max(150.0, float64(numItems)/6.0))
	numWebPages := int(math.Max(8000.0, float64(numCustomers)/2.5))

	businessScaleFactor := math.Sqrt(float64(numCustomers) / 100000.0)

	stores := int(math.Max(8.0, 15.0*businessScaleFactor))
	warehouses := int(math.Max(5.0, 12.0*businessScaleFactor))
	callCenters := int(math.Max(3.0, 8.0*businessScaleFactor))
	webSites := int(math.Max(3.0, 10.0*businessScaleFactor))
	catalogPages := int(math.Max(80.0, 200.0*businessScaleFactor))

	customerDemographics := int(math.Max(800.0, float64(numCustomers)*0.025))
	householdDemographics := int(math.Max(500.0, float64(numCustomers)*0.018))
	incomeBands := int(math.Max(10.0, 15.0*businessScaleFactor))
	reasons := int(math.Max(20.0, 35.0*businessScaleFactor))
	shipModes := int(math.Max(6.0, 10.0*businessScaleFactor))

	counts := ECommerceDSRowCounts{
		StoreSales:   numStoreSales,
		WebSales:     numWebSales,
		CatalogSales: numCatalogSales,

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

// CalculateECommerceRowCounts determines the target number of rows for each e-commerce table.
func CalculateECommerceRowCounts(targetGB float64) (ECommerceRowCounts, error) {
	if targetGB <= 0 {
		return ECommerceRowCounts{}, fmt.Errorf("target size must be positive")
	}

	targetBytes := targetGB * 1024 * 1024 * 1024

	const effectiveSizePerOrderItem = 65.0

	rows := math.Max(1, math.Round(targetBytes/effectiveSizePerOrderItem))
	if math.IsNaN(rows) || rows > math.MaxInt32 {
		return ECommerceRowCounts{}, fmt.Errorf("target size exceeds supported row count")
	}
	numOrderItems := int(rows)

	numOrderHeaders := int(math.Max(1.0, math.Round(float64(numOrderItems)/AvgItemsPerOrder)))
	numCustomers := int(math.Max(1.0, math.Round(float64(numOrderHeaders)/DefaultOrdersPerCustomerRatio)))
	numCustomerAddresses := int(math.Max(1.0, math.Round(float64(numCustomers)*AvgAddressesPerCustomer)))

	numSuppliers := int(math.Max(1.0, math.Round(float64(numCustomers)/100.0)))
	numProducts := int(math.Max(1.0, math.Round(float64(numSuppliers)*DefaultProductsPerSupplierRatio)))

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
func CalculateFinancialRowCounts(targetGB float64) (financialsimulation.FinancialRowCounts, error) {
	if targetGB <= 0 {
		return financialsimulation.FinancialRowCounts{}, fmt.Errorf("target size must be positive")
	}

	targetBytes := targetGB * 1024 * 1024 * 1024

	const effectiveSizePerDailyStockPrice = 139.0

	rows := math.Max(1, math.Round(targetBytes/effectiveSizePerDailyStockPrice))
	if math.IsNaN(rows) || rows >= float64(math.MaxInt) || math.Round(rows/(AvgTradingDaysPerYear*NumYearsOfData)) > math.MaxInt32 {
		return financialsimulation.FinancialRowCounts{}, fmt.Errorf("target size exceeds supported row count")
	}
	numPrices := int(rows)

	numCompanies := int(math.Max(1.0, math.Round(float64(numPrices)/(AvgTradingDaysPerYear*NumYearsOfData))))
	numExchanges := 5

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

	const effectiveSizePerAppointment = 200.0

	rows := math.Max(1, math.Round(targetBytes/effectiveSizePerAppointment))
	if math.IsNaN(rows) || rows >= float64(math.MaxInt) || math.Round(rows/5) > math.MaxInt32 {
		return medicalsimulation.MedicalRowCounts{}, fmt.Errorf("target size exceeds supported row count")
	}
	numAppointments := int(rows)
	numPatients := int(math.Max(1.0, math.Round(float64(numAppointments)/5.0)))
	numDoctors := int(math.Max(1.0, math.Round(float64(numPatients)/100.0)))
	numClinics := int(math.Max(1.0, math.Round(float64(numDoctors)/20.0)))

	counts := medicalsimulation.MedicalRowCounts{
		Patients:     numPatients,
		Doctors:      numDoctors,
		Clinics:      numClinics,
		Appointments: numAppointments,
	}

	return counts, nil
}
