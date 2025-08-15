package formats

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"time"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
)

// WriteSliceData dispatches writing based on format.
func WriteSliceData(data interface{}, filenameBase, format, outputDir string) error {
	targetFilename := filepath.Join(outputDir, filenameBase+"."+format)
	

	var writeErr error
	switch format {
	case "csv":
		writeErr = writeSliceToCSV(data, targetFilename)
	case "json":
		writeErr = writeSliceToJSON(data, targetFilename)
	case "parquet":
		writeErr = writeSliceToParquet(data, targetFilename)
	default:
		writeErr = fmt.Errorf("unsupported format '%s'", format)
	}
	if writeErr != nil {
		return fmt.Errorf("error writing %s: %w", targetFilename, writeErr)
	}
	return nil
}

// ValueToString converts a reflect.Value to its string representation, primarily for CSV.
func ValueToString(v reflect.Value) string {
	if !v.IsValid() {
		return ""
	}

	if v.Kind() == reflect.Ptr {
		if v.IsNil() {
			return ""
		}
		v = v.Elem()
	}

	if v.Kind() == reflect.Interface {
		if v.IsNil() {
			return ""
		}
		v = v.Elem()
	}

	switch v.Kind() {
	case reflect.String:
		return v.String()
	case reflect.Int, reflect.Int8, reflect.Int64:
		return strconv.FormatInt(v.Int(), 10)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return strconv.FormatUint(v.Uint(), 10)
	case reflect.Float32, reflect.Float64:
		return strconv.FormatFloat(v.Float(), 'f', -1, 64)
	case reflect.Bool:
		return strconv.FormatBool(v.Bool())
	case reflect.Struct:
		if t, ok := v.Interface().(time.Time); ok {
			return t.Format(time.RFC3339)
		}
		fmt.Fprintf(os.Stderr, "Warning: encountered unhandled struct type %s in ValueToString\n", v.Type())
		return fmt.Sprintf("[unhandled struct: %v]", v.Interface())
	default:
		fmt.Fprintf(os.Stderr, "Warning: encountered unhandled type %s in ValueToString\n", v.Kind())
		return fmt.Sprintf("[unhandled type: %v]", v.Interface())
	}
}

// AppendValueToBuilder appends a Go value to the correct Arrow builder.
func AppendValueToBuilder(bldr array.Builder, val reflect.Value) error {
	if val.Kind() == reflect.Ptr {
		if val.IsNil() {
			bldr.AppendNull()
			return nil
		}
		val = val.Elem()
	}
	if val.Kind() == reflect.Interface {
		if val.IsNil() {
			bldr.AppendNull()
			return nil
		}
		val = val.Elem()
	}
	if !val.IsValid() {
		bldr.AppendNull()
		return nil
	}
	switch bldr.(type) {
	case *array.Int32Builder:
		bldr.(*array.Int32Builder).Append(int32(val.Int()))
	case *array.Int64Builder:
		bldr.(*array.Int64Builder).Append(val.Int())
	case *array.Float32Builder:
		bldr.(*array.Float32Builder).Append(float32(val.Float()))
	case *array.Float64Builder:
		bldr.(*array.Float64Builder).Append(val.Float())
	case *array.StringBuilder:
		bldr.(*array.StringBuilder).Append(val.String())
	case *array.BooleanBuilder:
		bldr.(*array.BooleanBuilder).Append(val.Bool())
	case *array.TimestampBuilder:
		if t, ok := val.Interface().(time.Time); ok {
			bldr.(*array.TimestampBuilder).Append(arrow.Timestamp(t.UnixMicro()))
		} else {
			return fmt.Errorf("expected time.Time for TimestampBuilder")
		}
	default:
		return fmt.Errorf("unsupported arrow builder type: %T", bldr)
	}
	return nil
}
