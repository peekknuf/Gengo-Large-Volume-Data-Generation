package formats

import (
	"fmt"
	"reflect"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)



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
