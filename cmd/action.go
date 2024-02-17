package cmd

import (
	"fmt"
	"strconv"
	"strings"
	"sync"

	lipgloss "github.com/charmbracelet/lipgloss"
)

var logoStyle = lipgloss.NewStyle().
	Foreground(lipgloss.Color("#8A2BE2")).
	Italic(true).
	Bold(true)

var (
	outputFilename string
	selectedCols   = []string{
		"ID",
		"Timestamp",
		"ProductName",
		"Company",
		"Price",
		"Quantity",
		"Discount",
		"TotalPrice",
		"CustomerID",
		"FirstName",
		"LastName",
		"Email",
		"Address",
		"City",
		"State",
		"Zip",
		"Country",
	}
)

func getUserInput() (int, string, error) {

	var numRowsStr string
	// parsing underscores in the input
	// 0s are tricky and so writing it as 100_000_000 is far more comfortable.
	fmt.Print("Enter the number of rows (preferably a big one): ")
	if _, err := fmt.Scanln(&numRowsStr); err != nil {
		return 0, "", err
	}
	numRowsStr = strings.ReplaceAll(numRowsStr, "_", "")

	numRows, err := strconv.Atoi(numRowsStr)
	if err != nil {
		return 0, "", err
	}

	fmt.Print("Enter the output filename(without extension): ")
	if _, err := fmt.Scanln(&outputFilename); err != nil {
		return 0, "", err
	}
	// as for now only csv is supported, in the future might add parquet as well
	// it would make a lot of sense for analytical purposes and would greatly reduce size
	outputFilename += ".csv"

	return numRows, outputFilename, nil
}

func GenerateData(numRows int, outputFilename string, selectedCols []string) {
	ch := make(chan Row, 1000)

	var wg sync.WaitGroup

	wg.Add(1)
	go simulatingData(numRows, selectedCols, &wg, ch)

	wg.Add(1)
	go WriteToCSV(outputFilename, ch, &wg, selectedCols)

	wg.Wait()
	// so as there's functionality for writing the input w the underscores
	// the output is gonna be with underscores regardless of the input
	//once again, better readability. 123123123 can be hard, but 123_123_123 is miles better.
	numRowsWithUnderscores := addUnderscores(numRows)

	fmt.Printf("Generated %s rows of e-commerce data and saved to %s\n", numRowsWithUnderscores, outputFilename)
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
