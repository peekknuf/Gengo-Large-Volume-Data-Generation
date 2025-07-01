// cmd/json.go
package formats

import (
	"encoding/json"
	"fmt"
	"os"
	"reflect"
)

// writeSliceToJSON writes a slice of structs to a file using JSON Lines format.
func writeSliceToJSON(data interface{}, targetFilename string) error {
	// 1. Validate input
	sliceVal := reflect.ValueOf(data)
	if sliceVal.Kind() != reflect.Slice {
		return fmt.Errorf("writeSliceToJSON expected a slice, got %T", data)
	}
	sliceLen := sliceVal.Len() // Store length
	if sliceLen == 0 {
		fmt.Printf("Skipping JSON write for %s: slice is empty.\n", targetFilename)
		return nil
	}

	// 2. Create file
	file, err := os.Create(targetFilename)
	if err != nil {
		return fmt.Errorf("failed to create json file %s: %w", targetFilename, err)
	}
	// Use named return variable to capture potential close error
	var fileCloseErr error
	defer func() {
		if err := file.Close(); err != nil {
			fileCloseErr = fmt.Errorf("error closing json file %s: %w", targetFilename, err)
		}
	}()

	// 3. Create Encoder (JSON Lines format - no indentation)
	encoder := json.NewEncoder(file)

	// 4. Iterate and encode
	elemType := sliceVal.Type().Elem()
	isPointer := elemType.Kind() == reflect.Ptr

	for i := 0; i < sliceLen; i++ {
		elemVal := sliceVal.Index(i)
		if isPointer && elemVal.IsNil() {
			fmt.Fprintf(os.Stderr, "Warning: skipping nil JSON element at index %d in %s\n", i, targetFilename)
			continue
		}

		interfaceVal := elemVal.Interface() // Encoder works on interface{}

		if err := encoder.Encode(interfaceVal); err != nil {
			// Return encoding error immediately, defer will still close file
			return fmt.Errorf("failed to encode record %d to json file %s: %w", i, targetFilename, err)
		}
	}

	// If encoding finished, return potential file close error captured by defer
	if fileCloseErr != nil {
		return fileCloseErr
	}

	fmt.Printf("Successfully wrote %d records to %s (JSON Lines)\n", sliceLen, targetFilename)
	return nil // Success
}
