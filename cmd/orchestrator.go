// cmd/orchestrator.go
package cmd

import (
	"fmt"
	"os"
	"time"
)

// GenerateModelData orchestrates the sequential generation and writing of the relational model.
func GenerateModelData(modelType string, counts interface{}, format string, outputDir string) error {
	startTime := time.Now()

	err := os.MkdirAll(outputDir, 0755)
	if err != nil {
		return fmt.Errorf("error creating output directory %s: %w", outputDir, err)
	}
	fmt.Printf("Ensured output directory exists: %s\n", outputDir)

	switch modelType {
	case "ecommerce":
		ecommerceCounts := counts.(ECommerceRowCounts)
		err = GenerateECommerceModelData(ecommerceCounts, format, outputDir)
	case "financial":
		financialCounts := counts.(FinancialRowCounts)
		err = GenerateFinancialModelData(financialCounts, format, outputDir)
	default:
		err = fmt.Errorf("unsupported model type: %s", modelType)
	}

	if err != nil {
		return err
	}

	fmt.Printf("\nTotal model generation completed in %s.\n", time.Since(startTime).Round(time.Second))
	return nil
}

// GenerateECommerceModelData generates and writes the e-commerce relational model.
func GenerateECommerceModelData(counts ECommerceRowCounts, format string, outputDir string) error {
	customers := generateCustomers(counts.Customers)
	customerAddresses := generateCustomerAddresses(customers)
	suppliers := generateSuppliers(counts.Suppliers)
	supplierIDs := make([]int, len(suppliers))
	for i, s := range suppliers {
		supplierIDs[i] = s.SupplierID
	}
	products := generateProducts(counts.Products, supplierIDs)

	if err := writeSliceData(customers, "dim_customers", format, outputDir); err != nil {
		return fmt.Errorf("failed writing customers: %w", err)
	}
	if err := writeSliceData(customerAddresses, "dim_customer_addresses", format, outputDir); err != nil {
		return fmt.Errorf("failed writing customer addresses: %w", err)
	}
	if err := writeSliceData(suppliers, "dim_suppliers", format, outputDir); err != nil {
		return fmt.Errorf("failed writing suppliers: %w", err)
	}
	if err := writeSliceData(products, "dim_products", format, outputDir); err != nil {
		return fmt.Errorf("failed writing products: %w", err)
	}

	customerIDs := make([]int, len(customers))
	for i, c := range customers {
		customerIDs[i] = c.CustomerID
	}
	customers = nil // Free memory

	productInfo := make(map[int]ProductDetails, len(products))
	productIDsForSampling := make([]int, len(products))
	for i, p := range products {
		productInfo[p.ProductID] = ProductDetails{BasePrice: p.BasePrice}
		productIDsForSampling[i] = p.ProductID
	}
	products = nil // Free memory

	if err := generateAndWriteFacts(counts.OrderHeaders, customerIDs, customerAddresses, productInfo, productIDsForSampling, format, outputDir); err != nil {
		return fmt.Errorf("failed generating/writing facts: %w", err)
	}
	return nil
}

func GenerateFinancialModelData(counts FinancialRowCounts, format string, outputDir string) error {
	companies := generateCompanies(counts.Companies)
	exchanges := generateExchanges(counts.Exchanges)

	if err := writeSliceData(companies, "dim_companies", format, outputDir); err != nil {
		return fmt.Errorf("failed writing companies: %w", err)
	}
	if err := writeSliceData(exchanges, "dim_exchanges", format, outputDir); err != nil {
		return fmt.Errorf("failed writing exchanges: %w", err)
	}

	if err := generateAndWriteDailyStockPrices(counts.DailyStockPrices, companies, exchanges, format, outputDir); err != nil {
		return fmt.Errorf("failed generating/writing daily stock prices: %w", err)
	}
	return nil
}
