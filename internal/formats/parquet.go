package formats

import (
	"fmt"
	"os"
	"reflect"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

const (
	parquetWriteBatchSize = 1024 * 64
)

func WriteSliceToParquet(data interface{}, targetFilename string) (err error) {
	sliceVal := reflect.ValueOf(data)
	if sliceVal.Kind() != reflect.Slice {
		return fmt.Errorf("WriteSliceToParquet expected a slice, got %T", data)
	}
	sliceLen := sliceVal.Len()
	if sliceLen == 0 {
		fmt.Printf("Skipping Parquet write for %s: slice is empty.\n", targetFilename)
		return nil
	}

	// Handle []interface{} case by getting the type of the first element
	var elemType reflect.Type
	var isPointer bool

	if sliceVal.Type().Elem().Kind() == reflect.Interface {
		if sliceLen > 0 {
			firstElem := sliceVal.Index(0).Interface()
			elemType = reflect.TypeOf(firstElem)
			isPointer = elemType.Kind() == reflect.Ptr
			if isPointer {
				elemType = elemType.Elem()
			}
		} else {
			return fmt.Errorf("WriteSliceToParquet cannot determine type from empty []interface{}")
		}
	} else {
		elemType = sliceVal.Type().Elem()
		isPointer = elemType.Kind() == reflect.Ptr
		if isPointer {
			elemType = elemType.Elem()
		}
	}

	if elemType.Kind() != reflect.Struct {
		return fmt.Errorf("WriteSliceToParquet expected slice of structs/pointers, got %s", elemType.Kind())
	}
	schema, err := buildArrowSchema(elemType)
	if err != nil {
		return fmt.Errorf("failed to build schema for %s: %w", elemType.Name(), err)
	}
	fieldMap := map[string]int{}
	for i := 0; i < elemType.NumField(); i++ {
		field := elemType.Field(i)
		name := getParquetFieldName(field)
		if name != "-" {
			fieldMap[name] = i
		}
	}

	var file *os.File
	file, err = os.Create(targetFilename)
	if err != nil {
		return fmt.Errorf("failed to create parquet file %s: %w", targetFilename, err)
	}
	defer func() {
		if err != nil && file != nil {
			_ = file.Close()
		}
	}()

	pool := memory.NewGoAllocator()
	props := parquet.NewWriterProperties(parquet.WithDictionaryDefault(true), parquet.WithCompression(compress.Codecs.Snappy))
	arrowProps := pqarrow.NewArrowWriterProperties()
	var writer *pqarrow.FileWriter
	writer, err = pqarrow.NewFileWriter(schema, file, props, arrowProps)
	if err != nil {
		return fmt.Errorf("failed to create parquet writer for %s: %w", targetFilename, err)
	}
	defer func() {
		if closeErr := writer.Close(); closeErr != nil && err == nil {
			err = fmt.Errorf("error closing parquet writer for %s: %w", targetFilename, closeErr)
		}
	}()
	builder := array.NewRecordBuilder(pool, schema)
	defer builder.Release()

	rowsInCurrentBatch := 0
	fmt.Printf("Writing %d records to %s (Parquet)...\n", sliceLen, targetFilename)
	for i := 0; i < sliceLen; i++ {
		elemVal := sliceVal.Index(i)

		// Handle []interface{} case - need to extract the actual value from the interface
		if sliceVal.Type().Elem().Kind() == reflect.Interface {
			if elemVal.IsNil() {
				continue
			}
			// Get the actual value from the interface
			actualValue := elemVal.Interface()
			elemVal = reflect.ValueOf(actualValue)
			if isPointer {
				if elemVal.IsNil() {
					continue
				}
				elemVal = elemVal.Elem()
			}
		} else {
			// Regular slice case
			if isPointer {
				if elemVal.IsNil() {
					continue
				}
				elemVal = elemVal.Elem()
			}
		}

		if !elemVal.IsValid() || elemVal.Kind() != reflect.Struct {
			continue
		}
		for fieldIdx, arrowField := range schema.Fields() {
			structFieldIndex, ok := fieldMap[arrowField.Name]
			if !ok {
				err = fmt.Errorf("schema field '%s' not found", arrowField.Name)
				return err
			}
			fieldVal := elemVal.Field(structFieldIndex)
			if appendErr := AppendValueToBuilder(builder.Field(fieldIdx), fieldVal); appendErr != nil {
				err = fmt.Errorf("append field %s record %d: %w", arrowField.Name, i, appendErr)
				return err
			}
		}
		rowsInCurrentBatch++
		if rowsInCurrentBatch >= parquetWriteBatchSize {
			if err = WriteTypedBatch(writer, builder, targetFilename); err != nil {
				return err
			}
			rowsInCurrentBatch = 0
		}
	}

	if rowsInCurrentBatch > 0 {
		fmt.Printf("Writing final %d rows for %s parquet...\n", rowsInCurrentBatch, targetFilename)
		if err = WriteTypedBatch(writer, builder, targetFilename); err != nil {
			return err
		}
	}

	if err == nil {
		fmt.Printf("Successfully finished writing to %s (pending close)\n", targetFilename)
	}
	return err
}

// getParquetFieldName determines the field name from tags or struct field name.
func getParquetFieldName(field reflect.StructField) string {
	for _, tagName := range []string{"parquet", "json", "csv"} {
		tag := field.Tag.Get(tagName)
		if tag != "" {
			parts := strings.Split(tag, ",")
			if parts[0] == "-" {
				return "-"
			}
			if parts[0] != "" {
				return parts[0]
			}
		}
	}
	return field.Name
}

// buildArrowSchema creates an Arrow schema from a Go struct type via reflection.
func buildArrowSchema(structType reflect.Type) (*arrow.Schema, error) {
	fields := make([]arrow.Field, 0, structType.NumField())
	for i := 0; i < structType.NumField(); i++ {
		field := structType.Field(i)
		fieldName := getParquetFieldName(field)
		if fieldName == "-" {
			continue
		}
		var arrowType arrow.DataType
		fieldType := field.Type
		if fieldType.Kind() == reflect.Ptr {
			fieldType = fieldType.Elem()
		}
		switch fieldType.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32:
			arrowType = arrow.PrimitiveTypes.Int32
		case reflect.Int64:
			arrowType = arrow.PrimitiveTypes.Int64
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32:
			arrowType = arrow.PrimitiveTypes.Int32 // Promote uint32 to int32
		case reflect.Uint64:
			arrowType = arrow.PrimitiveTypes.Int64 // Promote uint64 to int64
		case reflect.Float32:
			arrowType = arrow.PrimitiveTypes.Float32
		case reflect.Float64:
			arrowType = arrow.PrimitiveTypes.Float64
		case reflect.String:
			arrowType = arrow.BinaryTypes.String
		case reflect.Bool:
			arrowType = arrow.FixedWidthTypes.Boolean
		case reflect.Struct:
			if fieldType == reflect.TypeOf(time.Time{}) {
				arrowType = arrow.FixedWidthTypes.Timestamp_us
			} else {
				return nil, fmt.Errorf("unsupported struct field: %s", field.Name)
			}
		default:
			return nil, fmt.Errorf("unsupported field kind: %s", fieldType.Kind())
		}
		fields = append(fields, arrow.Field{Name: fieldName, Type: arrowType, Nullable: true})
	}
	return arrow.NewSchema(fields, nil), nil
}
