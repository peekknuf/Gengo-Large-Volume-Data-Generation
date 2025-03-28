// cmd/orchestrator.go
package cmd

import (
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// GenerateModelData orchestrates the sequential generation and writing of the relational model.
func GenerateModelData(counts RowCounts, format string, outputDir string) error {
	startTime := time.Now()

	// 1. Create output directory
	err := os.MkdirAll(outputDir, 0755)
	if err != nil { return fmt.Errorf("error creating output directory %s: %w", outputDir, err) }
	fmt.Printf("Ensured output directory exists: %s\n", outputDir)

	// --- Generate Dimensions ---
	fmt.Println("\n--- Generating Dimension Data ---")
	custGenStart := time.Now()
	customers := generateCustomers(counts.Customers)
	fmt.Printf("Generated %s customers in %s.\n", addUnderscores(len(customers)), time.Since(custGenStart).Round(time.Millisecond))

	prodGenStart := time.Now()
	products := generateProducts(counts.Products)
	fmt.Printf("Generated %s products in %s.\n", addUnderscores(len(products)), time.Since(prodGenStart).Round(time.Millisecond))

	locGenStart := time.Now()
	locations := generateLocations(counts.Locations)
	fmt.Printf("Generated %s locations in %s.\n", addUnderscores(len(locations)), time.Since(locGenStart).Round(time.Millisecond))

	// --- Write Dimensions ---
	fmt.Println("\n--- Writing Dimension Data ---")
	writeStart := time.Now()
	err = writeDimensionData(customers, "dim_customers", format, outputDir); if err != nil { return fmt.Errorf("failed writing customers: %w", err) }
	err = writeDimensionData(products, "dim_products", format, outputDir); if err != nil { return fmt.Errorf("failed writing products: %w", err) }
	err = writeDimensionData(locations, "dim_locations", format, outputDir); if err != nil { return fmt.Errorf("failed writing locations: %w", err) }
	fmt.Printf("Finished writing dimensions in %s.\n", time.Since(writeStart).Round(time.Millisecond))

	// --- Prepare FKs and Data ---
	fmt.Println("\n--- Preparing Data for Fact Generation ---")
	prepStart := time.Now()
	customerIDs := make([]int, len(customers)); for i, c := range customers { customerIDs[i] = c.CustomerID }; customers = nil
	productInfo := make(map[int]ProductDetails, len(products)); productIDsForSampling := make([]int, len(products))
	for i, p := range products { productInfo[p.ProductID] = ProductDetails{BasePrice: p.BasePrice}; productIDsForSampling[i] = p.ProductID }; products = nil
	locationIDs := make([]int, len(locations)); for i, l := range locations { locationIDs[i] = l.LocationID }; locations = nil
	fmt.Printf("Prepared FKs and supporting data in %s.\n", time.Since(prepStart).Round(time.Millisecond))

	// --- Generate and Write Facts ---
	fmt.Println("\n--- Generating and Writing Fact Data ---")
	err = generateAndWriteOrders(counts.Orders, customerIDs, productInfo, productIDsForSampling, locationIDs, format, outputDir)
	if err != nil { return fmt.Errorf("failed generating/writing orders: %w", err) }

	fmt.Printf("\nTotal model generation completed in %s.\n", time.Since(startTime).Round(time.Second))
	return nil // Overall success
}

// writeDimensionData dispatches writing based on format.
func writeDimensionData(data interface{}, filenameBase, format, outputDir string) error {
	targetFilename := filepath.Join(outputDir, filenameBase+"."+format)
	fmt.Printf("Attempting to write dimension to: %s\n", targetFilename)

	var writeErr error
	switch format {
	case "csv": writeErr = writeSliceToCSV(data, targetFilename)   // Assumes writers are directly in cmd/
	case "json": writeErr = writeSliceToJSON(data, targetFilename)
	case "parquet": writeErr = writeSliceToParquet(data, targetFilename)
	default: writeErr = fmt.Errorf("unsupported format '%s'", format)
	}
	if writeErr != nil { return fmt.Errorf("error writing %s: %w", targetFilename, writeErr) }
	return nil
}