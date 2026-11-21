package core

import (
	"fmt"
	financial "github.com/peekknuf/Gengo/internal/simulation/financial"
	medical "github.com/peekknuf/Gengo/internal/simulation/medical"
	"math"
	"reflect"
)

// Validate before allocating dimensions or starting generators that sample parent keys.
func validateRowPlan(model string, counts any) error {
	var name string
	switch counts.(type) {
	case ECommerceRowCounts:
		name = "ecommerce"
	case ECommerceDSRowCounts:
		name = "ecommerce-ds"
	case financial.FinancialRowCounts:
		name = "financial"
	case medical.MedicalRowCounts:
		name = "medical"
	default:
		return fmt.Errorf("unsupported row plan %T", counts)
	}
	if name != model {
		return fmt.Errorf("%s requires a matching row plan, got %T", model, counts)
	}
	v := reflect.ValueOf(counts)
	for i := 0; i < v.NumField(); i++ {
		field := v.Type().Field(i).Name
		n := v.Field(i).Int()
		if n < 0 {
			return fmt.Errorf("%s count must be nonnegative", field)
		}
		// Non-retail dimension keys and order IDs are int32 in the Parquet schema.
		if model != "ecommerce-ds" && field != "DailyStockPrices" && field != "Appointments" && n > math.MaxInt32 {
			return fmt.Errorf("%s count exceeds int32 key range", field)
		}
	}
	switch c := counts.(type) {
	case ECommerceRowCounts:
		if c.ProductCategories > 10 {
			return fmt.Errorf("at most 10 product categories are available")
		}
		if c.Products > 0 && (c.Suppliers == 0 || c.ProductCategories == 0) {
			return fmt.Errorf("products require suppliers and categories")
		}
		if c.CustomerAddresses < c.Customers || int64(c.CustomerAddresses) > 3*int64(c.Customers) {
			return fmt.Errorf("customers require 1 to 3 addresses each")
		}
		if c.OrderHeaders > 0 && (c.Products == 0 || c.Customers == 0) {
			return fmt.Errorf("orders require products and customers")
		}
		if c.OrderItems < c.OrderHeaders || int64(c.OrderItems) > 10*int64(c.OrderHeaders) {
			return fmt.Errorf("orders require 1 to 10 items each")
		}
	case medical.MedicalRowCounts:
		if c.Appointments > 0 && (c.Patients == 0 || c.Doctors == 0 || c.Clinics == 0) {
			return fmt.Errorf("appointments require patients, doctors and clinics")
		}
	case financial.FinancialRowCounts:
		if c.DailyStockPrices > 0 && (c.Companies == 0 || c.Exchanges == 0) {
			return fmt.Errorf("stock prices require companies and exchanges")
		}
	case ECommerceDSRowCounts:
		if c.Inventory != 0 || c.StoreReturns != 0 || c.WebReturns != 0 || c.CatalogReturns != 0 {
			return fmt.Errorf("retail inventory and returns are unsupported")
		}
		if c.HouseholdDemographics > 0 && c.IncomeBands == 0 {
			return fmt.Errorf("household demographics require income bands")
		}
		if c.Customers > 0 && (c.CustomerAddresses == 0 || c.CustomerDemographics == 0 || c.HouseholdDemographics == 0) {
			return fmt.Errorf("retail customers require addresses and demographic rows")
		}
		if c.WebPages > 0 && c.Customers == 0 {
			return fmt.Errorf("web pages require customers")
		}
		if c.Promotions > 0 && c.Items == 0 {
			return fmt.Errorf("promotions require items")
		}
		if (c.StoreSales > 0 || c.WebSales > 0 || c.CatalogSales > 0) && (c.Promotions == 0 || c.Items == 0 || c.Customers == 0) {
			return fmt.Errorf("retail sales require promotions, items and customers")
		}
		if c.StoreSales > 0 && c.Stores == 0 {
			return fmt.Errorf("store sales require stores")
		}
		if (c.WebSales > 0 || c.CatalogSales > 0) && (c.ShipModes == 0 || c.Warehouses == 0) {
			return fmt.Errorf("shipped sales require ship modes and warehouses")
		}
		if c.WebSales > 0 && (c.WebSites == 0 || c.WebPages == 0) {
			return fmt.Errorf("web sales require sites and pages")
		}
		if c.CatalogSales > 0 && (c.CallCenters == 0 || c.CatalogPages == 0) {
			return fmt.Errorf("catalog sales require call centers and pages")
		}
	}
	return nil
}
