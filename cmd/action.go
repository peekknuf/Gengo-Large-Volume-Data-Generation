package cmd

import (
	"fmt"
	"strconv"
	"strings"
	"sync"

	lipgloss "github.com/charmbracelet/lipgloss"
)

var outputFormat string

var logoStyle = lipgloss.NewStyle().
	Foreground(lipgloss.Color("#8A2BE2")).
	Italic(true).
	Bold(true)

var selectedCols = []string{
	"ID",
	"Timestamp",
	"ProductName",
	"Company",
	"Price",
	"Quantity",
	"Discount",
	"FirstName",
	"LastName",
	"Email",
	"Address",
	"City",
	"State",
	"Zip",
	"Country",
	"OrderStatus",
	"PaymentMethod",
	"ShippingAddress",
	"ProductCategory",
}

// --- User Input ---

func getUserInput() (numRows int, outputTarget string, format string, err error) {
	// parsing underscores in the input
	var numRowsStr string
	fmt.Print("Enter the number of rows (e.g., 1_000_000): ")
	if _, scanErr := fmt.Scanln(&numRowsStr); scanErr != nil {
		err = fmt.Errorf("error reading number of rows: %w", scanErr)
		return
	}
	numRowsStr = strings.ReplaceAll(numRowsStr, "_", "")

	numRows, err = strconv.Atoi(numRowsStr)
	if err != nil {
		err = fmt.Errorf("invalid number format: %w", err)
		return
	}
	if numRows <= 0 {
		err = fmt.Errorf("number of rows must be positive")
		return
	}

	fmt.Print("Enter the desired format (csv/json/parquet): ")
	if _, scanErr := fmt.Scanln(&outputFormat); scanErr != nil {
		err = fmt.Errorf("error reading output format: %w", scanErr)
		return
	}
	outputFormat = strings.ToLower(strings.TrimSpace(outputFormat)) // Normalize format

	var baseName string
	prompt := "Enter the output filename (without extension): "
	if outputFormat == "parquet" {
		prompt = "Enter the output directory name: "
	}

	fmt.Print(prompt)
	if _, scanErr := fmt.Scanln(&baseName); scanErr != nil {
		err = fmt.Errorf("error reading output name: %w", scanErr)
		return
	}
	baseName = strings.TrimSpace(baseName)
	if baseName == "" {
		err = fmt.Errorf("output name cannot be empty")
		return
	}

	// Assign outputTarget based on format
	if outputFormat == "csv" || outputFormat == "json" {
		outputTarget = baseName + "." + outputFormat
		format = outputFormat
	} else if outputFormat == "parquet" {
		outputTarget = baseName // Use the base name as the directory path
		format = outputFormat
	} else {
		err = fmt.Errorf("unsupported output format: %s", outputFormat)
		return
	}

	return numRows, outputTarget, format, nil
}

func GenerateData(numRows int, outputTarget string, format string, selectedCols []string) {
	// Use a buffered channel
	// Buffer size can be tuned. Larger might help if writer is slower than generator.
	ch := make(chan Row, 500)

	var wg sync.WaitGroup

	// Start producer goroutine
	wg.Add(1)
	go simulatingData(numRows, selectedCols, &wg, ch)

	// Start consumer (writer) goroutine based on format
	wg.Add(1)
	switch format {
	case "csv":
		fmt.Printf("Starting CSV writer for %s...\n", outputTarget)
		go WriteToCSV(outputTarget, ch, &wg, selectedCols)
	case "json":
		fmt.Printf("Starting JSON writer for %s...\n", outputTarget)
		go WriteToJSON(outputTarget, ch, &wg, selectedCols)
	case "parquet":
		fmt.Printf("Starting Parquet writer for directory %s...\n", outputTarget)
		go WriteToParquet(outputTarget, ch, &wg, selectedCols) // Pass directory path
	default:
		// Should not happen if getUserInput validation is correct, but handle defensively
		fmt.Printf("Error: Invalid format '%s' passed to GenerateData. Aborting write.\n", format)
		wg.Done() // Decrement waitgroup as no writer was started
		// Drain channel to prevent producer deadlock
		go func() {
			for range ch {
			}
		}()
	}

	wg.Wait() // Wait for producer and consumer to finish

	// --- Final Output Message ---
	numRowsWithUnderscores := addUnderscores(numRows)
	if format == "parquet" {
		fmt.Printf("\nSuccessfully generated %s rows and saved to directory '%s'\n", numRowsWithUnderscores, outputTarget)
	} else {
		fmt.Printf("\nSuccessfully generated %s rows and saved to file '%s'\n", numRowsWithUnderscores, outputTarget)
	}
}

// ouput readability
func addUnderscores(n int) string {
	str := strconv.Itoa(n)
	var parts []string
	for len(str) > 3 {
		parts = append(parts, str[len(str)-3:])
		str = str[:len(str)-3]
	}
	if len(str) > 0 {
		parts = append(parts, str)
	}
	for i := len(parts)/2 - 1; i >= 0; i-- {
		opp := len(parts) - 1 - i
		parts[i], parts[opp] = parts[opp], parts[i]
	}
	return strings.Join(parts, "_")
}

const logo = `
___          ___     
/ __|___ _ _ / __|___ 
| (_ / -_) ' \ (_ / _ \
\___\___|_||_\___\___/
					  
`
