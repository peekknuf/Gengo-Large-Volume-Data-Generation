package cmd

import (
	"fmt"
	"sync"

	sc "github.com/peekknuf/go_cli/gen"
)

var (
	numRows        int
	outputFilename string
	selectedCols   []string
)

func GenerateData(numRows int, outputFilename string, selectedCols []string) {
	ch := make(chan sc.Row, 100000)

	var wg sync.WaitGroup

	wg.Add(1)
	go sc.GenerateData(numRows, selectedCols, &wg, ch)

	wg.Add(1)
	go sc.WriteToCSV(outputFilename, ch, &wg, selectedCols)

	wg.Wait()

	fmt.Printf("Generated %d rows of e-commerce data and saved to %s\n", numRows, outputFilename)
}
