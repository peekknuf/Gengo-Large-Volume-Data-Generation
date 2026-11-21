package formats

import (
	"fmt"
	"os"

	ecommercemodels "github.com/peekknuf/Gengo/internal/models/ecommerce"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

const typedWriteBatchSize = 65536

func CreateTypedParquetWriter(schema *arrow.Schema, targetFilename string) (*os.File, *pqarrow.FileWriter, *array.RecordBuilder, error) {
	file, err := os.Create(targetFilename)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to create parquet file %s: %w", targetFilename, err)
	}

	pool := memory.NewGoAllocator()
	props := parquet.NewWriterProperties(parquet.WithDictionaryDefault(false), parquet.WithCompression(compress.Codecs.Snappy))
	arrowProps := pqarrow.NewArrowWriterProperties()
	writer, err := pqarrow.NewFileWriter(schema, file, props, arrowProps)
	if err != nil {
		file.Close()
		return nil, nil, nil, fmt.Errorf("failed to create parquet writer for %s: %w", targetFilename, err)
	}

	builder := array.NewRecordBuilder(pool, schema)
	return file, writer, builder, nil
}

// WriteTypedBatch writes and releases a record. NewRecord resets the builders
// for reuse by the next batch.
func WriteTypedBatch(writer *pqarrow.FileWriter, builder *array.RecordBuilder, targetFilename string) error {
	record := builder.NewRecord()
	defer record.Release()
	if err := writer.Write(record); err != nil {
		return fmt.Errorf("error writing batch to parquet file %s: %w", targetFilename, err)
	}
	return nil
}

func WriteOrderHeadersToParquetTyped(headers []ecommercemodels.OrderHeader, targetFilename string) (err error) {
	if len(headers) == 0 {
		return nil
	}

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "order_id", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "customer_id", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "shipping_address_id", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "billing_address_id", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "order_timestamp", Type: arrow.FixedWidthTypes.Timestamp_us, Nullable: false},
		{Name: "order_status", Type: arrow.BinaryTypes.String, Nullable: false},
	}, nil)

	file, writer, builder, err := CreateTypedParquetWriter(schema, targetFilename)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil && file != nil {
			_ = file.Close()
		}
	}()
	defer func() {
		if closeErr := writer.Close(); closeErr != nil && err == nil {
			err = fmt.Errorf("error closing parquet writer for %s: %w", targetFilename, closeErr)
		}
	}()
	defer builder.Release()

	b0 := builder.Field(0).(*array.Int32Builder)
	b1 := builder.Field(1).(*array.Int32Builder)
	b2 := builder.Field(2).(*array.Int32Builder)
	b3 := builder.Field(3).(*array.Int32Builder)
	b4 := builder.Field(4).(*array.TimestampBuilder)
	b5 := builder.Field(5).(*array.StringBuilder)

	fmt.Printf("Writing %d records to %s (Parquet)...\n", len(headers), targetFilename)

	for i, h := range headers {
		b0.Append(int32(h.OrderID))
		b1.Append(int32(h.CustomerID))
		b2.Append(int32(h.ShippingAddressID))
		b3.Append(int32(h.BillingAddressID))
		b4.Append(arrow.Timestamp(h.OrderTimestamp.UnixMicro()))
		b5.Append(h.OrderStatus)

		if (i+1)%typedWriteBatchSize == 0 {
			if err = WriteTypedBatch(writer, builder, targetFilename); err != nil {
				return err
			}
		}
	}

	if len(headers)%typedWriteBatchSize != 0 {
		if err = WriteTypedBatch(writer, builder, targetFilename); err != nil {
			return err
		}
	}

	fmt.Printf("Successfully finished writing to %s\n", targetFilename)
	return nil
}

func WriteOrderItemsToParquetTyped(items []ecommercemodels.OrderItem, targetFilename string) (err error) {
	if len(items) == 0 {
		return nil
	}

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "order_item_id", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "order_id", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "product_id", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "quantity", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "unit_price", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
		{Name: "discount", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
	}, nil)

	file, writer, builder, err := CreateTypedParquetWriter(schema, targetFilename)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil && file != nil {
			_ = file.Close()
		}
	}()
	defer func() {
		if closeErr := writer.Close(); closeErr != nil && err == nil {
			err = fmt.Errorf("error closing parquet writer for %s: %w", targetFilename, closeErr)
		}
	}()
	defer builder.Release()

	b0 := builder.Field(0).(*array.Int32Builder)
	b1 := builder.Field(1).(*array.Int32Builder)
	b2 := builder.Field(2).(*array.Int32Builder)
	b3 := builder.Field(3).(*array.Int32Builder)
	b4 := builder.Field(4).(*array.Float64Builder)
	b5 := builder.Field(5).(*array.Float64Builder)

	fmt.Printf("Writing %d records to %s (Parquet)...\n", len(items), targetFilename)

	for i, item := range items {
		b0.Append(int32(item.OrderItemID))
		b1.Append(int32(item.OrderID))
		b2.Append(int32(item.ProductID))
		b3.Append(int32(item.Quantity))
		b4.Append(item.UnitPrice)
		b5.Append(item.Discount)

		if (i+1)%typedWriteBatchSize == 0 {
			if err = WriteTypedBatch(writer, builder, targetFilename); err != nil {
				return err
			}
		}
	}

	if len(items)%typedWriteBatchSize != 0 {
		if err = WriteTypedBatch(writer, builder, targetFilename); err != nil {
			return err
		}
	}

	fmt.Printf("Successfully finished writing to %s\n", targetFilename)
	return nil
}
