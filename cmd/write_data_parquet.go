package cmd

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/apache/arrow/go/v12/arrow/memory" // Correct import path
	"github.com/apache/arrow/go/v12/parquet"
	"github.com/apache/arrow/go/v12/parquet/compress"
	"github.com/apache/arrow/go/v12/parquet/pqarrow"
)

const (
	// batchSize determines how many rows are collected in memory before writing a batch to the current Parquet file.
	batchSize = 1024 * 64 // 64k rows per batch write within a file

	// maxRowsPerFile determines when to close the current Parquet file and start a new one.
	maxRowsPerFile = 1024 * 1024 // 1 million rows per file part
)

func WriteToParquet(outputDir string, ch <-chan Row, wg *sync.WaitGroup, selectedCols []string) {
	defer wg.Done()

	// 1. Ensure the output directory exists
	err := os.MkdirAll(outputDir, 0755)
	if err != nil {
		fmt.Printf("Error creating output directory %s: %v\n", outputDir, err)
		for range ch { // Drain channel
		}
		return
	}

	// 2. Create schema fields based on selected columns
	fields := make([]arrow.Field, len(selectedCols))
	colNameToIndex := make(map[string]int, len(selectedCols))

	for i, col := range selectedCols {
		colNameToIndex[col] = i
		switch col {
		case "ID", "Quantity":
			fields[i] = arrow.Field{Name: col, Type: arrow.PrimitiveTypes.Int32}
		case "Price", "Discount", "TotalPrice":
			fields[i] = arrow.Field{Name: col, Type: arrow.PrimitiveTypes.Float64}
		case "Timestamp":
			// Correction: Use FixedWidthTypes for Timestamp
			fields[i] = arrow.Field{Name: col, Type: arrow.FixedWidthTypes.Timestamp_us}
		default: // All other fields assumed to be strings
			fields[i] = arrow.Field{Name: col, Type: arrow.BinaryTypes.String}
		}
	}
	schema := arrow.NewSchema(fields, nil)

	// 3. Set up writer properties with Snappy compression
	writerProps := parquet.NewWriterProperties(
		parquet.WithDictionaryDefault(true),
		parquet.WithCompression(compress.Codecs.Snappy),
	)
	arrowProps := pqarrow.NewArrowWriterProperties()

	// 4. Variables for managing multiple files and batches
	pool := memory.NewGoAllocator()
	var currentFile *os.File
	var currentWriter *pqarrow.FileWriter
	var currentBuilder *array.RecordBuilder
	fileCounter := -1
	rowsInCurrentFile := 0
	rowsInCurrentBatch := 0

	// Helper function to close resources safely
	closeCurrentResources := func(final bool) { // Add 'final' flag for clarity on when to check length
		if currentBuilder != nil {
			// Correction: Check length using Field(0).Len()
			// Only write if it's the final call *and* there's data in the builder
			builderLen := 0
			if len(currentBuilder.Fields()) > 0 { // Ensure builder has fields before accessing
				builderLen = currentBuilder.Field(0).Len()
			}

			if final && builderLen > 0 {
				// fmt.Printf("Writing final %d rows to %s\n", builderLen, currentFile.Name()) // Debug print
				record := currentBuilder.NewRecord()
				if currentWriter != nil { // Ensure writer is still valid
					if writeErr := currentWriter.Write(record); writeErr != nil {
						fmt.Printf("Error writing final batch to Parquet file %s: %v\n", currentFile.Name(), writeErr)
					}
				} else {
					fmt.Printf("Error: Attempted final write but Parquet writer was nil for file %s\n", currentFile.Name())
				}
				record.Release()
			}
			currentBuilder.Release()
			currentBuilder = nil
		}
		if currentWriter != nil {
			// fmt.Printf("Closing writer for %s\n", currentFile.Name()) // Debug print
			if closeErr := currentWriter.Close(); closeErr != nil {
				// Avoid error if file is already closed, check file name existence
				fileName := "unknown file"
				if currentFile != nil {
					fileName = currentFile.Name()
				}
				fmt.Printf("Error closing Parquet writer for file %s: %v\n", fileName, closeErr)
			}
			currentWriter = nil
		}
		if currentFile != nil {
			// fmt.Printf("Closing file %s\n", currentFile.Name()) // Debug print
			fileName := currentFile.Name() // Store name before closing
			if closeErr := currentFile.Close(); closeErr != nil {
				fmt.Printf("Error closing Parquet file %s: %v\n", fileName, closeErr)
			}
			currentFile = nil
		}
	}
	// Ensure final resources are closed
	defer func() {
		// fmt.Println("Running deferred resource cleanup...") // Debug print
		closeCurrentResources(true) // Pass true to indicate final cleanup
	}()

	// 5. Process rows from channel
	for row := range ch {
		// Check if we need to start a new file
		if currentWriter == nil || rowsInCurrentFile >= maxRowsPerFile {
			// fmt.Printf("Condition met: new file needed (writer nil: %t, rowsInFile: %d >= %d)\n", currentWriter == nil, rowsInCurrentFile, maxRowsPerFile) // Debug
			closeCurrentResources(true) // Close previous resources, write remaining data

			fileCounter++
			rowsInCurrentFile = 0
			rowsInCurrentBatch = 0
			partFilename := filepath.Join(outputDir, fmt.Sprintf("part-%05d.parquet", fileCounter))
			// fmt.Printf("Creating new file: %s\n", partFilename) // Debug print

			var createErr error
			currentFile, createErr = os.Create(partFilename)
			if createErr != nil {
				fmt.Printf("Error creating Parquet part file %s: %v\n", partFilename, createErr)
				for range ch { // Drain channel
				}
				return // Exit function
			}

			var writerErr error
			currentWriter, writerErr = pqarrow.NewFileWriter(schema, currentFile, writerProps, arrowProps)
			if writerErr != nil {
				fmt.Printf("Error creating Parquet writer for file %s: %v\n", partFilename, writerErr)
				if currentFile != nil {
					_ = currentFile.Close()
					currentFile = nil
				}
				for range ch { // Drain channel
				}
				return // Exit function
			}

			// Create a new builder for the new file
			// fmt.Println("Creating new RecordBuilder") // Debug print
			currentBuilder = array.NewRecordBuilder(pool, schema)
		}

		// Defensive check for nil builder (should not happen if logic above is correct)
		if currentBuilder == nil {
			fmt.Println("Error: currentBuilder is nil unexpectedly. Skipping row.")
			rowsInCurrentFile++ // Still increment to potentially trigger file rollover if stuck
			continue
		}

		// 6. Append row data to the current builder
		for col, index := range colNameToIndex {
			// Add nil checks or default values if row fields can be nil/empty
			switch col {
			case "ID":
				currentBuilder.Field(index).(*array.Int32Builder).Append(int32(row.ID))
			case "Timestamp":
				currentBuilder.Field(index).(*array.TimestampBuilder).Append(arrow.Timestamp(row.Timestamp.UnixMicro()))
			case "ProductName":
				currentBuilder.Field(index).(*array.StringBuilder).Append(row.ProductName)
			case "Company":
				currentBuilder.Field(index).(*array.StringBuilder).Append(row.Company)
			case "Price":
				currentBuilder.Field(index).(*array.Float64Builder).Append(row.Price)
			case "Quantity":
				currentBuilder.Field(index).(*array.Int32Builder).Append(int32(row.Quantity))
			case "Discount":
				currentBuilder.Field(index).(*array.Float64Builder).Append(row.Discount)
			case "TotalPrice":
				currentBuilder.Field(index).(*array.Float64Builder).Append(row.TotalPrice)
			case "FirstName":
				currentBuilder.Field(index).(*array.StringBuilder).Append(row.FirstName)
			case "LastName":
				currentBuilder.Field(index).(*array.StringBuilder).Append(row.LastName)
			case "Email":
				currentBuilder.Field(index).(*array.StringBuilder).Append(row.Email)
			case "Address":
				currentBuilder.Field(index).(*array.StringBuilder).Append(row.Address)
			case "City":
				currentBuilder.Field(index).(*array.StringBuilder).Append(row.City)
			case "State":
				currentBuilder.Field(index).(*array.StringBuilder).Append(row.State)
			case "Zip":
				currentBuilder.Field(index).(*array.StringBuilder).Append(row.Zip)
			case "Country":
				currentBuilder.Field(index).(*array.StringBuilder).Append(row.Country)
			case "OrderStatus":
				currentBuilder.Field(index).(*array.StringBuilder).Append(row.OrderStatus)
			case "PaymentMethod":
				currentBuilder.Field(index).(*array.StringBuilder).Append(row.PaymentMethod)
			case "ShippingAddress":
				currentBuilder.Field(index).(*array.StringBuilder).Append(row.ShippingAddress)
			case "ProductCategory":
				currentBuilder.Field(index).(*array.StringBuilder).Append(row.ProductCategory)
			}
		}

		rowsInCurrentFile++
		rowsInCurrentBatch++

		// 7. Write batch if size is reached
		if rowsInCurrentBatch >= batchSize {
			// fmt.Printf("Writing batch (%d rows) to %s\n", rowsInCurrentBatch, currentFile.Name()) // Debug print
			record := currentBuilder.NewRecord()
			if err := currentWriter.Write(record); err != nil {
				fmt.Printf("Error writing batch to Parquet file %s: %v\n", currentFile.Name(), err)
				// Decide if error is fatal - for now, we just report and continue
			}
			record.Release()

			// Reset the builder for the next batch *within the same file*
			currentBuilder.Release()
			currentBuilder = array.NewRecordBuilder(pool, schema)
			rowsInCurrentBatch = 0
		}
	}

	// 8. Final write and close are handled by the deferred call to closeCurrentResources(true)
	fmt.Printf("Finished consuming channel for Parquet. Final cleanup via defer. Output dir: %s\n", outputDir)
}
