package formats

import (
	"fmt"
	"os"
	"reflect"
	"strings"
	"time"

	ecommercemodels "github.com/peekknuf/Gengo/internal/models/ecommerce"
	financialmodels "github.com/peekknuf/Gengo/internal/models/financial"
	medicalmodels "github.com/peekknuf/Gengo/internal/models/medical"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

const (
	parquetWriteBatchSize = 1024 * 16
)

func writeSliceToParquet(data interface{}, targetFilename string) (err error) {
	sliceVal := reflect.ValueOf(data)
	if sliceVal.Kind() != reflect.Slice {
		return fmt.Errorf("writeSliceToParquet expected a slice, got %T", data)
	}
	sliceLen := sliceVal.Len()
	if sliceLen == 0 {
		fmt.Printf("Skipping Parquet write for %s: slice is empty.\n", targetFilename)
		return nil
	}

	elemType := sliceVal.Type().Elem()
	isPointer := elemType.Kind() == reflect.Ptr
	if isPointer {
		elemType = elemType.Elem()
	}
	if elemType.Kind() != reflect.Struct {
		return fmt.Errorf("writeSliceToParquet expected slice of structs/pointers, got %s", elemType.Kind())
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
	defer func() { // Defer check/close only for error path before writer setup/close
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
	} // Error triggers file close defer
	defer func() { // Use DEFER for writer close
		if closeErr := writer.Close(); closeErr != nil && err == nil {
			err = fmt.Errorf("error closing parquet writer for %s: %w", targetFilename, closeErr)
		}
	}()
	builder := array.NewRecordBuilder(pool, schema)
	defer builder.Release()

	rowsInCurrentBatch := 0
	fmt.Printf("Writing %d records to %s (Parquet)...\n", sliceLen, targetFilename)
	progressStep := sliceLen / 20
	if progressStep == 0 {
		progressStep = 1
	}
	for i := 0; i < sliceLen; i++ {
		elemVal := sliceVal.Index(i)
		if isPointer {
			if elemVal.IsNil() {
				continue
			}
			elemVal = elemVal.Elem()
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
			newBuilder, writeErr := writeParquetBatchCorrected(writer, builder, pool, schema, targetFilename)
			if writeErr != nil {
				err = writeErr
				return err
			}
			builder = newBuilder
			rowsInCurrentBatch = 0
		}
		// if (i+1)%progressStep == 0 || i == sliceLen-1 {
		// fmt.Printf("... %s / %s records processed for %s\n", utils.AddUnderscores(i+1), utils.AddUnderscores(sliceLen), targetFilename)
		// }
	}

	if rowsInCurrentBatch > 0 {
		fmt.Printf("Writing final %d rows for %s parquet...\n", rowsInCurrentBatch, targetFilename)
		_, writeErr := writeParquetBatchCorrected(writer, builder, pool, schema, targetFilename)
		if writeErr != nil {
			err = writeErr
			return err
		}
	}

	if err == nil {
		fmt.Printf("Successfully finished writing to %s (pending close)\n", targetFilename)
	}
	return err
}

// writeParquetBatchCorrected writes a batch, releases the builder, returns a new one.
func writeParquetBatchCorrected(writer *pqarrow.FileWriter, builder *array.RecordBuilder, pool memory.Allocator, schema *arrow.Schema, errorContextFilename string) (*array.RecordBuilder, error) {
	record := builder.NewRecord()
	defer record.Release()
	if err := writer.Write(record); err != nil {
		return builder, fmt.Errorf("error writing batch to parquet file %s: %w", errorContextFilename, err)
	}
	builder.Release()
	newBuilder := array.NewRecordBuilder(pool, schema)
	return newBuilder, nil
}

// getParquetFieldName determines the field name from tags or struct field name.
func getParquetFieldName(field reflect.StructField) string {
	tag := field.Tag.Get("parquet")
	if tag != "" {
		parts := strings.Split(tag, ",")
		if parts[0] == "-" {
			return "-"
		}
		if parts[0] != "" {
			return parts[0]
		}
	}
	tag = field.Tag.Get("json")
	if tag != "" {
		parts := strings.Split(tag, ",")
		if parts[0] == "-" {
			return "-"
		}
		if parts[0] != "" {
			return parts[0]
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

// Parquet writer functions for e-commerce models

func WriteCustomersToParquet(customers []ecommercemodels.Customer, targetFilename string) error {
	return writeSliceToParquet(customers, targetFilename)
}

func WriteCustomerAddressesToParquet(addresses []ecommercemodels.CustomerAddress, targetFilename string) error {
	return writeSliceToParquet(addresses, targetFilename)
}

func WriteSuppliersToParquet(suppliers []ecommercemodels.Supplier, targetFilename string) error {
	return writeSliceToParquet(suppliers, targetFilename)
}

func WriteProductCategoriesToParquet(categories []ecommercemodels.ProductCategory, targetFilename string) error {
	return writeSliceToParquet(categories, targetFilename)
}

func WriteProductsToParquet(products []ecommercemodels.Product, targetFilename string) error {
	return writeSliceToParquet(products, targetFilename)
}

func WriteOrderHeadersToParquet(headers []ecommercemodels.OrderHeader, targetFilename string) error {
	return writeSliceToParquet(headers, targetFilename)
}

func WriteOrderItemsToParquet(items []ecommercemodels.OrderItem, targetFilename string) error {
	return writeSliceToParquet(items, targetFilename)
}

// Parquet writer functions for financial models

func WriteCompaniesToParquet(companies []financialmodels.Company, targetFilename string) error {
	return writeSliceToParquet(companies, targetFilename)
}

func WriteExchangesToParquet(exchanges []financialmodels.Exchange, targetFilename string) error {
	return writeSliceToParquet(exchanges, targetFilename)
}

func WriteDailyStockPricesToParquet(prices []financialmodels.DailyStockPrice, targetFilename string) error {
	return writeSliceToParquet(prices, targetFilename)
}

// Parquet writer functions for medical models

func WritePatientsToParquet(patients []medicalmodels.Patient, targetFilename string) error {
	return writeSliceToParquet(patients, targetFilename)
}

func WriteDoctorsToParquet(doctors []medicalmodels.Doctor, targetFilename string) error {
	return writeSliceToParquet(doctors, targetFilename)
}

func WriteClinicsToParquet(clinics []medicalmodels.Clinic, targetFilename string) error {
	return writeSliceToParquet(clinics, targetFilename)
}

func WriteAppointmentsToParquet(appointments []medicalmodels.Appointment, targetFilename string) error {
	return writeSliceToParquet(appointments, targetFilename)
}
