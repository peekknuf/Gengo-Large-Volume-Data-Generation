package cmd

import (
	"log"
	"os"
	"sync"
	"time"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/apache/arrow/go/v12/arrow/memory"
	"github.com/apache/arrow/go/v12/parquet"
	"github.com/apache/arrow/go/v12/parquet/pqarrow"
)

func WriteToParquet(filename string, ch <-chan Row, wg *sync.WaitGroup, selectedCols []string) {
	defer wg.Done()

	// Create a new file
	f, err := os.Create(filename)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	// Create schema fields based on selected columns
	fields := make([]arrow.Field, len(selectedCols))
	for i, col := range selectedCols {
		switch col {
		case "ID", "Quantity":
			fields[i] = arrow.Field{Name: col, Type: arrow.PrimitiveTypes.Int32}
		case "Price", "Discount", "TotalPrice":
			fields[i] = arrow.Field{Name: col, Type: arrow.PrimitiveTypes.Float64}
		case "Timestamp":
			fields[i] = arrow.Field{Name: col, Type: arrow.BinaryTypes.String}
		default:
			fields[i] = arrow.Field{Name: col, Type: arrow.BinaryTypes.String}
		}
	}

	schema := arrow.NewSchema(fields, nil)

	// Set up writer properties
	writerProps := parquet.NewWriterProperties(parquet.WithDictionaryDefault(true))
	arrowProps := pqarrow.NewArrowWriterProperties()

	// Create Arrow writer
	writer, err := pqarrow.NewFileWriter(schema, f, writerProps, arrowProps)
	if err != nil {
		log.Fatal(err)
	}
	defer writer.Close()

	// Collect rows and write in batches
	pool := memory.NewGoAllocator()
	builder := array.NewRecordBuilder(pool, schema)
	defer builder.Release()

	rowCount := 0
	batchSize := 1000 // Adjust batch size as needed

	for row := range ch {
		for i, col := range selectedCols {
			switch col {
			case "ID":
				builder.Field(i).(*array.Int32Builder).Append(int32(row.ID))
			case "Timestamp":
				builder.Field(i).(*array.StringBuilder).Append(row.Timestamp.Format(time.RFC3339))
			case "ProductName":
				builder.Field(i).(*array.StringBuilder).Append(row.ProductName)
			case "Company":
				builder.Field(i).(*array.StringBuilder).Append(row.Company)
			case "Price":
				builder.Field(i).(*array.Float64Builder).Append(row.Price)
			case "Quantity":
				builder.Field(i).(*array.Int32Builder).Append(int32(row.Quantity))
			case "Discount":
				builder.Field(i).(*array.Float64Builder).Append(row.Discount)
			case "TotalPrice":
				builder.Field(i).(*array.Float64Builder).Append(row.TotalPrice)
			case "FirstName":
				builder.Field(i).(*array.StringBuilder).Append(row.FirstName)
			case "LastName":
				builder.Field(i).(*array.StringBuilder).Append(row.LastName)
			case "Email":
				builder.Field(i).(*array.StringBuilder).Append(row.Email)
			case "Address":
				builder.Field(i).(*array.StringBuilder).Append(row.Address)
			case "City":
				builder.Field(i).(*array.StringBuilder).Append(row.City)
			case "State":
				builder.Field(i).(*array.StringBuilder).Append(row.State)
			case "Zip":
				builder.Field(i).(*array.StringBuilder).Append(row.Zip)
			case "Country":
				builder.Field(i).(*array.StringBuilder).Append(row.Country)
			case "OrderStatus":
				builder.Field(i).(*array.StringBuilder).Append(row.OrderStatus)
			case "PaymentMethod":
				builder.Field(i).(*array.StringBuilder).Append(row.PaymentMethod)
			case "ShippingAddress":
				builder.Field(i).(*array.StringBuilder).Append(row.ShippingAddress)
			case "ProductCategory":
				builder.Field(i).(*array.StringBuilder).Append(row.ProductCategory)
			}
		}

		rowCount++
		if rowCount >= batchSize {
			record := builder.NewRecord()
			if err := writer.Write(record); err != nil {
				log.Fatal(err)
			}
			record.Release()
			// Create a new builder for the next batch
			builder.Release()
			builder = array.NewRecordBuilder(pool, schema)
			rowCount = 0
		}
	}

	// Write remaining records
	if rowCount > 0 {
		record := builder.NewRecord()
		if err := writer.Write(record); err != nil {
			log.Fatal(err)
		}
		record.Release()
	}
}
