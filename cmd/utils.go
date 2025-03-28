// cmd/utils.go
package cmd

import (
	"fmt"
	"os" // Needed for valueToString logging
	"reflect"
	"strconv"
	"strings"
	"time"
)

// addUnderscores enhances integer readability by adding underscore separators.
func addUnderscores(n int) string {
	str := strconv.Itoa(n)
	ln := len(str)
	if ln <= 3 {
		return str
	}

	var parts []string
	for ln > 3 {
		parts = append(parts, str[ln-3:])
		str = str[:ln-3]
		ln = len(str)
	}
	if ln > 0 {
		parts = append(parts, str)
	}
	// Reverse the parts
	for i, j := 0, len(parts)-1; i < j; i, j = i+1, j-1 {
		parts[i], parts[j] = parts[j], parts[i]
	}
	return strings.Join(parts, "_")
}

// valueToString converts a reflect.Value to its string representation, primarily for CSV.
func valueToString(v reflect.Value) string {
	// Handle invalid value gracefully
	if !v.IsValid() {
		return ""
	}

	// Handle pointers: check for nil, then dereference
	if v.Kind() == reflect.Ptr {
		if v.IsNil() {
			return ""
		}
		v = v.Elem()
	}

	// Handle interface values
	if v.Kind() == reflect.Interface {
		if v.IsNil() {
			return ""
		}
		v = v.Elem() // Get the concrete value held by the interface
	}

	// Convert based on Kind
	switch v.Kind() {
	case reflect.String:
		return v.String()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return strconv.FormatInt(v.Int(), 10)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return strconv.FormatUint(v.Uint(), 10)
	case reflect.Float32, reflect.Float64:
		// 'f' format, -1 precision (minimum needed), 64-bit representation
		return strconv.FormatFloat(v.Float(), 'f', -1, 64)
	case reflect.Bool:
		return strconv.FormatBool(v.Bool())
	case reflect.Struct:
		// Special handling for time.Time
		if t, ok := v.Interface().(time.Time); ok {
			// RFC3339 is a good standard choice: "2006-01-02T15:04:05Z07:00"
			return t.Format(time.RFC3339)
		}
		// Fallback for unexpected structs - might indicate an issue
		fmt.Fprintf(os.Stderr, "Warning: encountered unhandled struct type %s in valueToString\n", v.Type())
		return fmt.Sprintf("[unhandled struct: %v]", v.Interface())
	default:
		// Fallback for other types (slices, maps, etc.) - generally shouldn't be direct fields in our models
		fmt.Fprintf(os.Stderr, "Warning: encountered unhandled type %s in valueToString\n", v.Kind())
		return fmt.Sprintf("[unhandled type: %v]", v.Interface())
	}
}
