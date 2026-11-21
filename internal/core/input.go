package core

import (
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"

	financialsimulation "github.com/peekknuf/Gengo/internal/simulation/financial"
	medicalsimulation "github.com/peekknuf/Gengo/internal/simulation/medical"
	"github.com/peekknuf/Gengo/internal/utils"
)

func GetUserInput(modelTypeFlag string, targetGBFlag float64, formatFlag, outputDirFlag string, rowOverrides ...string) (modelType string, counts interface{}, format string, outputDir string, err error) {
	// --- Model Type ---
	modelType = modelTypeFlag
	if modelType == "" {
		fmt.Print("Enter the data model to generate (ecommerce/ecommerce-ds/financial/medical): ")
		if _, scanErr := fmt.Scanln(&modelType); scanErr != nil {
			err = fmt.Errorf("error reading model type: %w", scanErr)
			return
		}
	}
	modelType = strings.ToLower(strings.TrimSpace(modelType))
	modelType, err = matchModelType(modelType)
	if err != nil {
		return
	}

	// --- Target Size ---
	targetGB := targetGBFlag
	if targetGB == 0 {
		var targetGBStr string
		fmt.Print("Enter the approximate target size in GB (e.g., 0.5, 10): ")
		if _, scanErr := fmt.Scanln(&targetGBStr); scanErr != nil {
			err = fmt.Errorf("error reading target size: %w", scanErr)
			return
		}
		targetGB, err = strconv.ParseFloat(strings.TrimSpace(targetGBStr), 64)
		if err != nil {
			err = fmt.Errorf("invalid number format for GB: %w", err)
			return
		}
	}
	if targetGB <= 0 || math.IsNaN(targetGB) || math.IsInf(targetGB, 0) {
		err = fmt.Errorf("target size must be finite and positive")
		return
	}

	// --- Output Format (determined before row counts for format-aware sizing) ---
	format = formatFlag
	if format == "" {
		fmt.Print("Enter the desired output format (csv/parquet): ")
		if _, scanErr := fmt.Scanln(&format); scanErr != nil {
			err = fmt.Errorf("error reading output format: %w", scanErr)
			return
		}
	}
	format = strings.ToLower(strings.TrimSpace(format))
	if format != "csv" && format != "parquet" {
		err = fmt.Errorf("unsupported output format: %s (choose csv or parquet)", format)
		return
	}

	// --- Calculate Row Counts ---
	switch modelType {
	case "ecommerce":
		var ecommerceCounts ECommerceRowCounts
		ecommerceCounts, err = CalculateECommerceRowCounts(targetGB)
		if err != nil {
			err = fmt.Errorf("error calculating e-commerce row counts: %w", err)
			return
		}
		if err = applyRowOverrides(&ecommerceCounts, rowOverrides); err != nil {
			return
		}
		if err = validateRowPlan(modelType, ecommerceCounts); err != nil {
			return
		}
		counts = ecommerceCounts
		fmt.Println("\n--- Resolved E-commerce Row Counts ---")
		fmt.Printf("Customers:         %s\n", utils.AddUnderscores(ecommerceCounts.Customers))
		fmt.Printf("Customer Addresses: %s\n", utils.AddUnderscores(ecommerceCounts.CustomerAddresses))
		fmt.Printf("Suppliers:         %s\n", utils.AddUnderscores(ecommerceCounts.Suppliers))
		fmt.Printf("Products:          %s\n", utils.AddUnderscores(ecommerceCounts.Products))
		fmt.Printf("Product Categories: %s\n", utils.AddUnderscores(ecommerceCounts.ProductCategories))
		fmt.Printf("Order Headers:     %s\n", utils.AddUnderscores(ecommerceCounts.OrderHeaders))
		fmt.Printf("Order Items:       %s\n", utils.AddUnderscores(ecommerceCounts.OrderItems))
	case "ecommerce-ds":
		var ecommerceDSCounts ECommerceDSRowCounts
		ecommerceDSCounts, err = CalculateECommerceDSRowCounts(targetGB, format)
		if err != nil {
			err = fmt.Errorf("error calculating e-commerce-ds row counts: %w", err)
			return
		}
		if err = applyRowOverrides(&ecommerceDSCounts, rowOverrides); err != nil {
			return
		}
		if err = validateRowPlan(modelType, ecommerceDSCounts); err != nil {
			return
		}
		counts = ecommerceDSCounts
		fmt.Println("\n--- Resolved E-commerce DS (TPC-DS) Row Counts ---")
		fmt.Println("\n?? FACT TABLES:")
		fmt.Printf("Store Sales:       %s\n", utils.AddUnderscores(ecommerceDSCounts.StoreSales))
		fmt.Printf("Web Sales:         %s\n", utils.AddUnderscores(ecommerceDSCounts.WebSales))
		fmt.Printf("Catalog Sales:     %s\n", utils.AddUnderscores(ecommerceDSCounts.CatalogSales))
		fmt.Println("Returns and inventory generation are not implemented.")

		fmt.Println("\n?? CORE DIMENSIONS:")
		fmt.Printf("Customers:         %s\n", utils.AddUnderscores(ecommerceDSCounts.Customers))
		fmt.Printf("Items:             %s\n", utils.AddUnderscores(ecommerceDSCounts.Items))
		fmt.Printf("Customer Addresses: %s\n", utils.AddUnderscores(ecommerceDSCounts.CustomerAddresses))
		fmt.Printf("Promotions:        %s\n", utils.AddUnderscores(ecommerceDSCounts.Promotions))
		fmt.Printf("Web Pages:         %s\n", utils.AddUnderscores(ecommerceDSCounts.WebPages))

		fmt.Println("\n?? BUSINESS DIMENSIONS:")
		fmt.Printf("Stores:            %s\n", utils.AddUnderscores(ecommerceDSCounts.Stores))
		fmt.Printf("Warehouses:        %s\n", utils.AddUnderscores(ecommerceDSCounts.Warehouses))
		fmt.Printf("Call Centers:      %s\n", utils.AddUnderscores(ecommerceDSCounts.CallCenters))
		fmt.Printf("Web Sites:         %s\n", utils.AddUnderscores(ecommerceDSCounts.WebSites))
		fmt.Printf("Catalog Pages:     %s\n", utils.AddUnderscores(ecommerceDSCounts.CatalogPages))

		fmt.Println("\n?? DEMOGRAPHIC DIMENSIONS:")
		fmt.Printf("Customer Demographics: %s\n", utils.AddUnderscores(ecommerceDSCounts.CustomerDemographics))
		fmt.Printf("Household Demographics: %s\n", utils.AddUnderscores(ecommerceDSCounts.HouseholdDemographics))
		fmt.Printf("Income Bands:      %s\n", utils.AddUnderscores(ecommerceDSCounts.IncomeBands))

		fmt.Println("\n?? OPERATIONAL DIMENSIONS:")
		fmt.Printf("Reasons:           %s\n", utils.AddUnderscores(ecommerceDSCounts.Reasons))
		fmt.Printf("Ship Modes:        %s\n", utils.AddUnderscores(ecommerceDSCounts.ShipModes))

		// Calculate business metrics
		totalSales := ecommerceDSCounts.StoreSales + ecommerceDSCounts.WebSales + ecommerceDSCounts.CatalogSales
		avgOrdersPerCustomer := float64(totalSales) / float64(max(1, ecommerceDSCounts.Customers))

		fmt.Println("\n?? BUSINESS METRICS:")
		fmt.Printf("Total Sales:       %s\n", utils.AddUnderscores(totalSales))
		fmt.Printf("Orders/Customer:   %.1f\n", avgOrdersPerCustomer)
		fmt.Printf("Sales Distribution: Store %.0f%%, Web %.0f%%, Catalog %.0f%%\n",
			float64(ecommerceDSCounts.StoreSales)/float64(max(1, totalSales))*100,
			float64(ecommerceDSCounts.WebSales)/float64(max(1, totalSales))*100,
			float64(ecommerceDSCounts.CatalogSales)/float64(max(1, totalSales))*100)
	case "financial":
		var financialCounts financialsimulation.FinancialRowCounts
		financialCounts, err = CalculateFinancialRowCounts(targetGB)
		if err != nil {
			err = fmt.Errorf("error calculating financial row counts: %w", err)
			return
		}
		if err = applyRowOverrides(&financialCounts, rowOverrides); err != nil {
			return
		}
		if err = validateRowPlan(modelType, financialCounts); err != nil {
			return
		}
		counts = financialCounts
		fmt.Println("\n--- Resolved Financial Row Counts ---")
		fmt.Printf("Companies:             %s\n", utils.AddUnderscores(financialCounts.Companies))
		fmt.Printf("Exchanges:             %s\n", utils.AddUnderscores(financialCounts.Exchanges))
		fmt.Printf("Daily Stock Prices:    %s\n", utils.AddUnderscores(financialCounts.DailyStockPrices))
	case "medical":
		var medicalCounts medicalsimulation.MedicalRowCounts
		medicalCounts, err = CalculateMedicalRowCounts(targetGB)
		if err != nil {
			err = fmt.Errorf("error calculating medical row counts: %w", err)
			return
		}
		if err = applyRowOverrides(&medicalCounts, rowOverrides); err != nil {
			return
		}
		if err = validateRowPlan(modelType, medicalCounts); err != nil {
			return
		}
		counts = medicalCounts
		fmt.Println("\n--- Resolved Medical Row Counts ---")
		fmt.Printf("Patients:      %s\n", utils.AddUnderscores(medicalCounts.Patients))
		fmt.Printf("Doctors:       %s\n", utils.AddUnderscores(medicalCounts.Doctors))
		fmt.Printf("Clinics:       %s\n", utils.AddUnderscores(medicalCounts.Clinics))
		fmt.Printf("Appointments:  %s\n", utils.AddUnderscores(medicalCounts.Appointments))
	}

	fmt.Println("----------------------------------------")
	fmt.Printf("Note: Final output size may vary due to data generation specifics and compression.\n\n")

	// --- Output Directory ---
	outputDir = outputDirFlag
	if outputDir == "" {
		fmt.Print("Enter the output directory name (will be created if it doesn't exist): ")
		if _, scanErr := fmt.Scanln(&outputDir); scanErr != nil {
			err = fmt.Errorf("error reading output directory name: %w", scanErr)
			return
		}
	}
	outputDir = strings.TrimSpace(outputDir)
	if outputDir == "" {
		err = fmt.Errorf("output directory name cannot be empty")
		return
	}

	return modelType, counts, format, outputDir, nil
}

// Overrides use the snake_case name of a row-plan field, e.g. daily_stock_prices.
func applyRowOverrides(counts any, overrides []string) error {
	if len(overrides) == 0 {
		return nil
	}
	plan := reflect.ValueOf(counts).Elem()
	fields := make(map[string]int, plan.NumField())
	for i := 0; i < plan.NumField(); i++ {
		fields[rowFieldName(plan.Type().Field(i).Name)] = i
	}
	for _, override := range overrides {
		key, raw, ok := strings.Cut(override, "=")
		field, known := fields[key]
		if !ok || !known {
			return fmt.Errorf("invalid row override %q: expected a known table=count", override)
		}
		n, err := strconv.Atoi(raw)
		if err != nil || n < 0 {
			return fmt.Errorf("invalid row count for %s: %q", key, raw)
		}
		plan.Field(field).SetInt(int64(n))
	}
	return nil
}

func matchModelType(input string) (string, error) {
	switch strings.ToLower(input) {
	case "ecommerce", "ecom", "e-commerce", "e":
		return "ecommerce", nil
	case "ecommerce-ds", "ecom-ds", "e-commerce-ds", "eds":
		return "ecommerce-ds", nil
	case "financial", "fin", "f":
		return "financial", nil
	case "medical", "med", "m":
		return "medical", nil
	default:
		return "", fmt.Errorf("unsupported model type: %s. Please choose ecommerce, ecommerce-ds, financial, or medical", input)
	}
}
