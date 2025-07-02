package core

import (
	"fmt"
	"os"
	"time"

	ecommercemodels "github.com/peekknuf/Gengo/internal/models/ecommerce"
	"github.com/peekknuf/Gengo/internal/simulation/ecommerce"
	financialsimulation "github.com/peekknuf/Gengo/internal/simulation/financial"
	medicalsimulation "github.com/peekknuf/Gengo/internal/simulation/medical"
	"github.com/peekknuf/Gengo/internal/formats"
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
		customers := ecommerce.GenerateCustomers(ecommerceCounts.Customers)
		customerIDs := make([]int, 0, len(customers))
		for _, c := range customers {
			customerIDs = append(customerIDs, c.CustomerID)
		}
		customerAddresses := ecommerce.GenerateCustomerAddresses(customers)
		suppliers := ecommerce.GenerateSuppliers(ecommerceCounts.Suppliers)
		supplierIDs := make([]int, 0, len(suppliers))
		for _, s := range suppliers {
			supplierIDs = append(supplierIDs, s.SupplierID)
		}
		products := ecommerce.GenerateProducts(ecommerceCounts.Products, supplierIDs)
		productInfo := make(map[int]ecommercemodels.ProductDetails, len(products))
		productIDsForSampling := make([]int, 0, len(products))
		for _, p := range products {
			productInfo[p.ProductID] = ecommercemodels.ProductDetails{BasePrice: p.BasePrice}
			productIDsForSampling = append(productIDsForSampling, p.ProductID)
		}
		
		if err = formats.WriteSliceData(customers, "dim_customers", format, outputDir); err != nil {
			return err
		}
		if err = formats.WriteSliceData(customerAddresses, "dim_customer_addresses", format, outputDir); err != nil {
			return err
		}
		if err = formats.WriteSliceData(suppliers, "dim_suppliers", format, outputDir); err != nil {
			return err
		}
		if err = formats.WriteSliceData(products, "dim_products", format, outputDir); err != nil {
			return err
		}
			var headers []ecommercemodels.OrderHeader
		var items []ecommercemodels.OrderItem
		headers, items, err = ecommerce.GenerateECommerceModelData(ecommerceCounts.OrderHeaders, customerIDs, customerAddresses, productInfo, productIDsForSampling)
		if err != nil {
			return err
		}
		if err = formats.WriteSliceData(headers, "fact_orders_header", format, outputDir); err != nil {
			return err
		}
		if err = formats.WriteSliceData(items, "fact_order_items", format, outputDir); err != nil {
			return err
		}
	case "financial":
		financialCounts := counts.(financialsimulation.FinancialRowCounts)
		companies := financialsimulation.GenerateCompanies(financialCounts.Companies)
		exchanges := financialsimulation.GenerateExchanges(financialCounts.Exchanges)

		// Write dimension data
		if err = formats.WriteSliceData(companies, "dim_companies", format, outputDir); err != nil {
			return err
		}
		if err = formats.WriteSliceData(exchanges, "dim_exchanges", format, outputDir); err != nil {
			return err
		}

		// Generate and write fact data
		err = financialsimulation.GenerateFinancialModelData(financialCounts, companies, exchanges, format, outputDir)
	case "medical":
		medicalCounts := counts.(medicalsimulation.MedicalRowCounts)
		patients := medicalsimulation.GeneratePatients(medicalCounts.Patients)
		doctors := medicalsimulation.GenerateDoctors(medicalCounts.Doctors)
		clinics := medicalsimulation.GenerateClinics(medicalCounts.Clinics)

		// Write dimension data
		if err = formats.WriteSliceData(patients, "dim_patients", format, outputDir); err != nil {
			return err
		}
		if err = formats.WriteSliceData(doctors, "dim_doctors", format, outputDir); err != nil {
			return err
		}
		if err = formats.WriteSliceData(clinics, "dim_clinics", format, outputDir); err != nil {
			return err
		}

		// Generate and write fact data
		err = medicalsimulation.GenerateMedicalModelData(medicalCounts, patients, doctors, clinics, format, outputDir)
	default:
		err = fmt.Errorf("unsupported model type: %s", modelType)
	}

	if err != nil {
		return err
	}

	fmt.Printf("\nTotal model generation completed in %s.\n", time.Since(startTime).Round(time.Second))
	return nil
}