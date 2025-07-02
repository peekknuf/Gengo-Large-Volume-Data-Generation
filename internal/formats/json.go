package formats

import (
	"encoding/json"
	"fmt"
	"os"
	"reflect"
)

func writeSliceToJSON(data interface{}, targetFilename string) error {
	sliceVal := reflect.ValueOf(data)
	if sliceVal.Kind() != reflect.Slice {
		return fmt.Errorf("writeSliceToJSON expected a slice, got %T", data)
	}
	sliceLen := sliceVal.Len()
	if sliceLen == 0 {
		fmt.Printf("Skipping JSON write for %s: slice is empty.\n", targetFilename)
		return nil
	}

	file, err := os.Create(targetFilename)
	if err != nil {
		return fmt.Errorf("failed to create json file %s: %w", targetFilename, err)
	}
	var fileCloseErr error
	defer func() {
		if err := file.Close(); err != nil {
			fileCloseErr = fmt.Errorf("error closing json file %s: %w", targetFilename, err)
		}
	}()

	encoder := json.NewEncoder(file)

	elemType := sliceVal.Type().Elem()
	isPointer := elemType.Kind() == reflect.Ptr

	for i := 0; i < sliceLen; i++ {
		elemVal := sliceVal.Index(i)
		if isPointer && elemVal.IsNil() {
			fmt.Fprintf(os.Stderr, "Warning: skipping nil JSON element at index %d in %s\n", i, targetFilename)
			continue
		}

		interfaceVal := elemVal.Interface()

		if err := encoder.Encode(interfaceVal); err != nil {
			return fmt.Errorf("failed to encode record %d to json file %s: %w", i, targetFilename, err)
		}
	}

	if fileCloseErr != nil {
		return fileCloseErr
	}

	fmt.Printf("Successfully wrote %d records to %s (JSON Lines)\n", sliceLen, targetFilename)
	return nil
}
