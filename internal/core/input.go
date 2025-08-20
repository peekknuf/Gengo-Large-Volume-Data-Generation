package core

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/peekknuf/Gengo/internal/utils"
	financialsimulation "github.com/peekknuf/Gengo/internal/simulation/financial"
	medicalsimulation "github.com/peekknuf/Gengo/internal/simulation/medical"
)

// getUserInputForModel prompts the user for the data model and then for the generation parameters.
func GetUserInputForModel() (modelType string, counts interface{}, format string, outputDir string, err error) {
	fmt.Print("Enter the data model to generate (ecommerce/financial/medical): ")
	if _, scanErr := fmt.Scanln(&modelType); scanErr != nil {
		err = fmt.Errorf("error reading model type: %w", scanErr)
		return
	}
	modelType = strings.ToLower(strings.TrimSpace(modelType))
	modelType, err = matchModelType(modelType)
	if err != nil {
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
		fmt.Printf("Customers:         %s\n", utils.AddUnderscores(ecommerceCounts.Customers))
		fmt.Printf("Customer Addresses: %s\n", utils.AddUnderscores(ecommerceCounts.CustomerAddresses))
		fmt.Printf("Suppliers:         %s\n", utils.AddUnderscores(ecommerceCounts.Suppliers))
		fmt.Printf("Products:          %s\n", utils.AddUnderscores(ecommerceCounts.Products))
		fmt.Printf("Product Categories: %s\n", utils.AddUnderscores(ecommerceCounts.ProductCategories))
		fmt.Printf("Order Headers:     %s\n", utils.AddUnderscores(ecommerceCounts.OrderHeaders))
		fmt.Printf("Order Items:       %s\n", utils.AddUnderscores(ecommerceCounts.OrderItems))
	case "financial":
		var financialCounts financialsimulation.FinancialRowCounts
		financialCounts, err = CalculateFinancialRowCounts(targetGB)
		if err != nil {
			err = fmt.Errorf("error calculating financial row counts: %w", err)
			return
		}
		counts = financialCounts
		fmt.Println("\n--- Estimated Financial Row Counts ---")
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
		counts = medicalCounts
		fmt.Println("\n--- Estimated Medical Row Counts ---")
		fmt.Printf("Patients:      %s\n", utils.AddUnderscores(medicalCounts.Patients))
		fmt.Printf("Doctors:       %s\n", utils.AddUnderscores(medicalCounts.Doctors))
		fmt.Printf("Clinics:       %s\n", utils.AddUnderscores(medicalCounts.Clinics))
		fmt.Printf("Appointments:  %s\n", utils.AddUnderscores(medicalCounts.Appointments))
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

// matchModelType attempts to match the user's input to a valid model type.
// It handles common misspellings and abbreviations.
func matchModelType(input string) (string, error) {
	switch strings.ToLower(input) {
	case "ecommerce", "ecom", "e-commerce":
		return "ecommerce", nil
	case "financial", "fin":
		return "financial", nil
	case "medical", "med":
		return "medical", nil
	default:
		return "", fmt.Errorf("unsupported model type: %s. Please choose ecommerce, financial, or medical", input)
	}
}
