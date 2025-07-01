// cmd/csv.go
package formats

import (
	"encoding/csv"
	"fmt"
	"os"
	"reflect"
	"strings"
)

// createRecordMap creates a map from CSV header names to their column indices.
func CreateRecordMap(records [][]string) map[string]int {
	header := records[0]
	rmap := make(map[string]int, len(header))
	for i, h := range header {
		rmap[h] = i
	}
	return rmap
}

// writeSliceToCSV writes a slice of structs to a CSV file.
// It uses reflection to determine headers (preferring 'json' tags) and data.
func writeSliceToCSV(data interface{}, targetFilename string) error {
	// 1. Validate input is a slice and not empty
	sliceVal := reflect.ValueOf(data)
	if sliceVal.Kind() != reflect.Slice {
		return fmt.Errorf("writeSliceToCSV expected a slice, got %T", data)
	}
	sliceLen := sliceVal.Len() // Store length
	if sliceLen == 0 {
		fmt.Printf("Skipping CSV write for %s: slice is empty.\n", targetFilename)
		return nil // Nothing to write
	}

	// 2. Create file
	file, err := os.Create(targetFilename)
	if err != nil {
		return fmt.Errorf("failed to create csv file %s: %w", targetFilename, err)
	}
	// Use named return variable to capture potential close error
	var fileCloseErr error
	defer func() {
		if err := file.Close(); err != nil {
			fileCloseErr = fmt.Errorf("error closing csv file %s: %w", targetFilename, err)
		}
	}()

	writer := csv.NewWriter(file)
	defer writer.Flush() // Flush before closing file

	// 3. Get headers using reflection
	elemType := sliceVal.Type().Elem()
	if elemType.Kind() == reflect.Ptr {
		elemType = elemType.Elem()
	}
	if elemType.Kind() != reflect.Struct {
		return fmt.Errorf("expected slice of structs/pointers, got %s", elemType.Kind())
	}

	numFields := elemType.NumField()
	headers := make([]string, numFields)
	fieldIndices := make([]int, numFields)

	for i := 0; i < numFields; i++ {
		field := elemType.Field(i)
		headerName := field.Name         // Default
		jsonTag := field.Tag.Get("json") // Prefer json tag
		if jsonTag != "" {
			parts := strings.Split(jsonTag, ",")
			if parts[0] != "" && parts[0] != "-" {
				headerName = parts[0]
			}
		} else {
			pqTag := field.Tag.Get("parquet")
			if pqTag != "" {
				parts := strings.Split(pqTag, ",")
				if parts[0] != "" && parts[0] != "-" {
					headerName = parts[0]
				}
			}
		} // Fallback to parquet tag
		headers[i] = headerName
		fieldIndices[i] = i
	}

	// 4. Write header row
	if err := writer.Write(headers); err != nil {
		return fmt.Errorf("failed to write csv header to %s: %w", targetFilename, err)
	}

	// 5. Iterate and write records
	record := make([]string, numFields)
	for i := 0; i < sliceLen; i++ {
		elemVal := sliceVal.Index(i)
		if elemVal.Kind() == reflect.Ptr {
			if elemVal.IsNil() {
				continue
			}
			elemVal = elemVal.Elem()
		}
		if !elemVal.IsValid() || elemVal.Kind() != reflect.Struct {
			continue
		}

		for j := 0; j < numFields; j++ {
			fieldVal := elemVal.Field(fieldIndices[j])
			record[j] = ValueToString(fieldVal) // Calls valueToString from formats.go
		}
		if err := writer.Write(record); err != nil {
			return fmt.Errorf("failed to write csv record %d to %s: %w", i, targetFilename, err)
		}
	}

	// 6. Check for writer errors (including flush) before file close defer runs
	if err := writer.Error(); err != nil {
		return fmt.Errorf("error occurred during csv writing/flushing to %s: %w", targetFilename, err)
	}

	// If writer was ok, return potential file close error captured by defer
	if fileCloseErr != nil {
		return fileCloseErr
	}

	fmt.Printf("Successfully wrote %d records to %s\n", sliceLen, targetFilename)
	return nil // Success
}
