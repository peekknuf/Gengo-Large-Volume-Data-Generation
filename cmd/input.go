// cmd/input.go
package cmd

import (
	"fmt"
	"strconv"
	"strings"
	// Assuming RowCounts is defined in sizing.go and addUnderscores in utils.go (same package)
)

// getUserInputForModel prompts the user for generation parameters based on target size.
// It returns the calculated row counts, desired output format, output directory path, and any error.
func getUserInputForModel() (counts RowCounts, format string, outputDir string, err error) {
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

	// --- Calculate Row Counts ---
	counts, err = CalculateRowCounts(targetGB) // Assumes CalculateRowCounts is in sizing.go
	if err != nil {
		err = fmt.Errorf("error calculating row counts: %w", err)
		return
	}

	// --- Display Estimated Counts ---
	fmt.Println("\n--- Estimated Row Counts ---")
	fmt.Printf("Customers: %s\n", addUnderscores(counts.Customers)) // Assumes addUnderscores is in utils.go
	fmt.Printf("Products:  %s\n", addUnderscores(counts.Products))
	fmt.Printf("Locations: %s\n", addUnderscores(counts.Locations))
	fmt.Printf("Orders:    %s\n", addUnderscores(counts.Orders))
	fmt.Println("----------------------------")
	fmt.Printf("Note: Final output size may vary due to data generation specifics and compression (especially Parquet).\n\n")

	// --- Get Output Format ---
	fmt.Print("Enter the desired output format (csv/json/parquet): ")
	if _, scanErr := fmt.Scanln(&format); scanErr != nil {
		err = fmt.Errorf("error reading output format: %w", scanErr)
		return
	}
	format = strings.ToLower(strings.TrimSpace(format))
	if format != "csv" && format != "json" && format != "parquet" {
		err = fmt.Errorf("unsupported output format: %s. Please choose csv, json, or parquet", format)
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

	return counts, format, outputDir, nil // Success
}
