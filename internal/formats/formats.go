package formats

import (
	"fmt"
	"path/filepath"
	"reflect"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	ecommercemodels "github.com/peekknuf/Gengo/internal/models/ecommerce"
	financialmodels "github.com/peekknuf/Gengo/internal/models/financial"
	medicalmodels "github.com/peekknuf/Gengo/internal/models/medical"
)

func WriteSliceData(data interface{}, filenameBase, format, outputDir string) error {
	targetFilename := filepath.Join(outputDir, filenameBase+"."+format)

	switch format {
	case "csv":
		switch v := data.(type) {
		// E-commerce
		case []ecommercemodels.Customer:
			return WriteCustomersToCSV(v, targetFilename)
		case []ecommercemodels.CustomerAddress:
			return WriteCustomerAddressesToCSV(v, targetFilename)
		case []ecommercemodels.Supplier:
			return WriteSuppliersToCSV(v, targetFilename)
		case []ecommercemodels.ProductCategory:
			return WriteProductCategoriesToCSV(v, targetFilename)
		case []ecommercemodels.Product:
			return WriteProductsToCSV(v, targetFilename)
		// Financial
		case []financialmodels.Company:
			return WriteCompaniesToCSV(v, targetFilename)
		case []financialmodels.Exchange:
			return WriteExchangesToCSV(v, targetFilename)
		// Medical
		case []medicalmodels.Patient:
			return WritePatientsToCSV(v, targetFilename)
		case []medicalmodels.Doctor:
			return WriteDoctorsToCSV(v, targetFilename)
		case []medicalmodels.Clinic:
			return WriteClinicsToCSV(v, targetFilename)
		case []financialmodels.DailyStockPrice:
			return WriteDailyStockPricesToCSV(v, targetFilename)
		case []medicalmodels.Appointment:
			return WriteAppointmentsToCSV(v, targetFilename)
		default:
			return fmt.Errorf("unsupported data type for CSV writing: %T", data)
		}
	case "json":
		// JSON and Parquet still use the old reflection-based method for now.
		return writeSliceToJSON(data, targetFilename)
	case "parquet":
		return writeSliceToParquet(data, targetFilename)
	default:
		return fmt.Errorf("unsupported format '%s'", format)
	}
}

// AppendValueToBuilder appends a Go value to the correct Arrow builder.
// This is kept for the Parquet writer.
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
