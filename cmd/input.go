package cmd

import (
	"fmt"
	"strconv"
	"strings"
)

// getUserInputForModel prompts the user for the data model and then for the generation parameters.
func getUserInputForModel() (modelType string, counts interface{}, format string, outputDir string, err error) {
	// --- Get Model Type ---
	fmt.Print("Enter the data model to generate (ecommerce/financial/medical): ")
	if _, scanErr := fmt.Scanln(&modelType); scanErr != nil {
		err = fmt.Errorf("error reading model type: %w", scanErr)
		return
	}
	modelType = strings.ToLower(strings.TrimSpace(modelType))
	if modelType != "ecommerce" && modelType != "financial" && modelType != "medical" {
		err = fmt.Errorf("unsupported model type: %s. Please choose ecommerce, financial, or medical", modelType)
		return
	}

	// --- Get Common Inputs ---
	var targetGBStr string
	fmt.Print("Enter the approximate target size in GB (e.g., 0.5, 10): ")
	if _, scanErr := fmt.Scanln(&targetGBStr); scanErr != nil {
		err = fmt.Errorf("error reading target size: %w", scanErr)
		return
	}

	targetGB, convErr := strconv.ParseFloat(strings.TrimSpace(targetGBStr), 64)
	if convErr != nil {
		err = fmt.Errorf("invalid number format for GB: %w", convErr)
		return
	}
	if targetGB <= 0 {
		err = fmt.Errorf("target size must be positive")
		return
	}

	// --- Calculate and Display Row Counts based on Model Type ---
	switch modelType {
	case "ecommerce":
		var ecommerceCounts ECommerceRowCounts
		ecommerceCounts, err = CalculateECommerceRowCounts(targetGB)
		if err != nil {
			err = fmt.Errorf("error calculating e-commerce row counts: %w", err)
			return
		}
		counts = ecommerceCounts
		fmt.Println("\n--- Estimated E-commerce Row Counts ---")
		fmt.Printf("Customers:         %s\n", addUnderscores(ecommerceCounts.Customers))
		fmt.Printf("Customer Addresses: %s\n", addUnderscores(ecommerceCounts.CustomerAddresses))
		fmt.Printf("Suppliers:         %s\n", addUnderscores(ecommerceCounts.Suppliers))
		fmt.Printf("Products:          %s\n", addUnderscores(ecommerceCounts.Products))
		fmt.Printf("Order Headers:     %s\n", addUnderscores(ecommerceCounts.OrderHeaders))
		fmt.Printf("Order Items:       %s\n", addUnderscores(ecommerceCounts.OrderItems))
	case "financial":
		var financialCounts FinancialRowCounts
		financialCounts, err = CalculateFinancialRowCounts(targetGB)
		if err != nil {
			err = fmt.Errorf("error calculating financial row counts: %w", err)
			return
		}
		counts = financialCounts
		fmt.Println("\n--- Estimated Financial Row Counts ---")
		fmt.Printf("Companies:             %s\n", addUnderscores(financialCounts.Companies))
		fmt.Printf("Exchanges:             %s\n", addUnderscores(financialCounts.Exchanges))
		fmt.Printf("Daily Stock Prices:    %s\n", addUnderscores(financialCounts.DailyStockPrices))
	case "medical":
		var medicalCounts MedicalRowCounts
		medicalCounts, err = CalculateMedicalRowCounts(targetGB)
		if err != nil {
			err = fmt.Errorf("error calculating medical row counts: %w", err)
			return
		}
		counts = medicalCounts
		fmt.Println("\n--- Estimated Medical Row Counts ---")
		fmt.Printf("Patients:      %s\n", addUnderscores(medicalCounts.Patients))
		fmt.Printf("Doctors:       %s\n", addUnderscores(medicalCounts.Doctors))
		fmt.Printf("Clinics:       %s\n", addUnderscores(medicalCounts.Clinics))
		fmt.Printf("Appointments:  %s\n", addUnderscores(medicalCounts.Appointments))
	}

	fmt.Println("----------------------------------------")
	fmt.Printf("Note: Final output size may vary due to data generation specifics and compression.\n\n")

	// --- Get Output Format ---
	fmt.Print("Enter the desired output format (csv/json/parquet): ")
	if _, scanErr := fmt.Scanln(&format); scanErr != nil {
		err = fmt.Errorf("error reading output format: %w", scanErr)
		return
	}
	format = strings.ToLower(strings.TrimSpace(format))
	if format != "csv" && format != "json" && format != "parquet" {
		err = fmt.Errorf("unsupported output format: %s", format)
		return
	}

	// --- Get Output Directory ---
	fmt.Print("Enter the output directory name (will be created if it doesn't exist): ")
	if _, scanErr := fmt.Scanln(&outputDir); scanErr != nil {
		err = fmt.Errorf("error reading output directory name: %w", scanErr)
		return
	}
	outputDir = strings.TrimSpace(outputDir)
	if outputDir == "" {
		err = fmt.Errorf("output directory name cannot be empty")
		return
	}

		return modelType, counts, format, outputDir, nil
}