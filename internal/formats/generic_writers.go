package formats

import (
	"fmt"
	"path/filepath"
	"strings"

	ecommercemodels "github.com/peekknuf/Gengo/internal/models/ecommerce"
	financialmodels "github.com/peekknuf/Gengo/internal/models/financial"
	medicalmodels "github.com/peekknuf/Gengo/internal/models/medical"
)

// WriteCustomers writes customer data to the specified format
func WriteCustomers(customers interface{}, outputDir string, format string) error {
	ext := getFileExtension(format)
	filename := filepath.Join(outputDir, "dim_customers"+ext)

	switch format {
	case "csv":
		return WriteCustomersToCSV(customers.([]ecommercemodels.Customer), filename)
	case "parquet":
		return WriteCustomersToParquet(customers.([]ecommercemodels.Customer), filename)
	default:
		return fmt.Errorf("unsupported format: %s", format)
	}
}

// WriteCustomerAddresses writes customer address data to the specified format
func WriteCustomerAddresses(addresses interface{}, outputDir string, format string) error {
	ext := getFileExtension(format)
	filename := filepath.Join(outputDir, "dim_customer_addresses"+ext)

	switch format {
	case "csv":
		return WriteCustomerAddressesToCSV(addresses.([]ecommercemodels.CustomerAddress), filename)
	case "parquet":
		return WriteCustomerAddressesToParquet(addresses.([]ecommercemodels.CustomerAddress), filename)
	default:
		return fmt.Errorf("unsupported format: %s", format)
	}
}

// WriteSuppliers writes supplier data to the specified format
func WriteSuppliers(suppliers interface{}, outputDir string, format string) error {
	ext := getFileExtension(format)
	filename := filepath.Join(outputDir, "dim_suppliers"+ext)

	switch format {
	case "csv":
		return WriteSuppliersToCSV(suppliers.([]ecommercemodels.Supplier), filename)
	case "parquet":
		return WriteSuppliersToParquet(suppliers.([]ecommercemodels.Supplier), filename)
	default:
		return fmt.Errorf("unsupported format: %s", format)
	}
}

// WriteProductCategories writes product category data to the specified format
func WriteProductCategories(categories interface{}, outputDir string, format string) error {
	ext := getFileExtension(format)
	filename := filepath.Join(outputDir, "dim_product_categories"+ext)

	switch format {
	case "csv":
		return WriteProductCategoriesToCSV(categories.([]ecommercemodels.ProductCategory), filename)
	case "parquet":
		return WriteProductCategoriesToParquet(categories.([]ecommercemodels.ProductCategory), filename)
	default:
		return fmt.Errorf("unsupported format: %s", format)
	}
}

// WriteProducts writes product data to the specified format
func WriteProducts(products interface{}, outputDir string, format string) error {
	ext := getFileExtension(format)
	filename := filepath.Join(outputDir, "dim_products"+ext)

	switch format {
	case "csv":
		return WriteProductsToCSV(products.([]ecommercemodels.Product), filename)
	case "parquet":
		return WriteProductsToParquet(products.([]ecommercemodels.Product), filename)
	default:
		return fmt.Errorf("unsupported format: %s", format)
	}
}

// WriteOrderHeaders writes order header data to the specified format
func WriteOrderHeaders(headers interface{}, outputDir string, format string) error {
	ext := getFileExtension(format)
	filename := filepath.Join(outputDir, "fact_orders_header"+ext)

	switch format {
	case "csv":
		return WriteStreamOrderHeadersToCSV(headers.(<-chan ecommercemodels.OrderHeader), filename)
	case "parquet":
		// For parquet, we expect a slice instead of a channel
		return WriteOrderHeadersToParquet(headers.([]ecommercemodels.OrderHeader), filename)
	default:
		return fmt.Errorf("unsupported format: %s", format)
	}
}

// WriteOrderItems writes order item data to the specified format
func WriteOrderItems(items interface{}, outputDir string, format string) error {
	ext := getFileExtension(format)
	filename := filepath.Join(outputDir, "fact_order_items"+ext)

	switch format {
	case "csv":
		return WriteStreamOrderItemsToCSV(items.(<-chan ecommercemodels.OrderItem), filename)
	case "parquet":
		// For parquet, we expect a slice instead of a channel
		return WriteOrderItemsToParquet(items.([]ecommercemodels.OrderItem), filename)
	default:
		return fmt.Errorf("unsupported format: %s", format)
	}
}

// Financial model writers

// WriteCompanies writes company data to the specified format
func WriteCompanies(companies interface{}, outputDir string, format string) error {
	ext := getFileExtension(format)
	filename := filepath.Join(outputDir, "dim_companies"+ext)

	switch format {
	case "csv":
		return WriteCompaniesToCSV(companies.([]financialmodels.Company), filename)
	case "parquet":
		return WriteCompaniesToParquet(companies.([]financialmodels.Company), filename)
	default:
		return fmt.Errorf("unsupported format: %s", format)
	}
}

// WriteExchanges writes exchange data to the specified format
func WriteExchanges(exchanges interface{}, outputDir string, format string) error {
	ext := getFileExtension(format)
	filename := filepath.Join(outputDir, "dim_exchanges"+ext)

	switch format {
	case "csv":
		return WriteExchangesToCSV(exchanges.([]financialmodels.Exchange), filename)
	case "parquet":
		return WriteExchangesToParquet(exchanges.([]financialmodels.Exchange), filename)
	default:
		return fmt.Errorf("unsupported format: %s", format)
	}
}

// WriteDailyStockPrices writes stock price data to the specified format
func WriteDailyStockPrices(prices interface{}, outputDir string, format string) error {
	ext := getFileExtension(format)
	filename := filepath.Join(outputDir, "fact_stock_prices"+ext)

	switch format {
	case "csv":
		return WriteDailyStockPricesToCSV(prices.([]financialmodels.DailyStockPrice), filename)
	case "parquet":
		return WriteDailyStockPricesToParquet(prices.([]financialmodels.DailyStockPrice), filename)
	default:
		return fmt.Errorf("unsupported format: %s", format)
	}
}

// Medical model writers

// WritePatients writes patient data to the specified format
func WritePatients(patients interface{}, outputDir string, format string) error {
	ext := getFileExtension(format)
	filename := filepath.Join(outputDir, "dim_patients"+ext)

	switch format {
	case "csv":
		return WritePatientsToCSV(patients.([]medicalmodels.Patient), filename)
	case "parquet":
		return WritePatientsToParquet(patients.([]medicalmodels.Patient), filename)
	default:
		return fmt.Errorf("unsupported format: %s", format)
	}
}

// WriteDoctors writes doctor data to the specified format
func WriteDoctors(doctors interface{}, outputDir string, format string) error {
	ext := getFileExtension(format)
	filename := filepath.Join(outputDir, "dim_doctors"+ext)

	switch format {
	case "csv":
		return WriteDoctorsToCSV(doctors.([]medicalmodels.Doctor), filename)
	case "parquet":
		return WriteDoctorsToParquet(doctors.([]medicalmodels.Doctor), filename)
	default:
		return fmt.Errorf("unsupported format: %s", format)
	}
}

// WriteClinics writes clinic data to the specified format
func WriteClinics(clinics interface{}, outputDir string, format string) error {
	ext := getFileExtension(format)
	filename := filepath.Join(outputDir, "dim_clinics"+ext)

	switch format {
	case "csv":
		return WriteClinicsToCSV(clinics.([]medicalmodels.Clinic), filename)
	case "parquet":
		return WriteClinicsToParquet(clinics.([]medicalmodels.Clinic), filename)
	default:
		return fmt.Errorf("unsupported format: %s", format)
	}
}

// WriteAppointments writes appointment data to the specified format
func WriteAppointments(appointments interface{}, outputDir string, format string) error {
	ext := getFileExtension(format)
	filename := filepath.Join(outputDir, "fact_appointments"+ext)

	switch format {
	case "csv":
		return WriteAppointmentsToCSV(appointments.([]medicalmodels.Appointment), filename)
	case "parquet":
		return WriteAppointmentsToParquet(appointments.([]medicalmodels.Appointment), filename)
	default:
		return fmt.Errorf("unsupported format: %s", format)
	}
}

// getFileExtension returns the appropriate file extension for the format
func getFileExtension(format string) string {
	switch strings.ToLower(format) {
	case "csv":
		return ".csv"
	case "parquet":
		return ".parquet"
	case "json":
		return ".jsonl"
	default:
		return ".csv" // default fallback
	}
}
