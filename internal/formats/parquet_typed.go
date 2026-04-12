package formats

import (
	"fmt"
	"os"

	ecommercemodels "github.com/peekknuf/Gengo/internal/models/ecommerce"
	ecommerceds "github.com/peekknuf/Gengo/internal/models/ecommerce-ds"
	financialmodels "github.com/peekknuf/Gengo/internal/models/financial"
	medicalmodels "github.com/peekknuf/Gengo/internal/models/medical"

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

func WriteStoreSalesToParquetTyped(rows []ecommerceds.StoreSales, targetFilename string) (err error) {
	if len(rows) == 0 {
		return nil
	}

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "ss_sold_date_sk", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "ss_sold_time_sk", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "ss_item_sk", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "ss_customer_sk", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "ss_cdemo_sk", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "ss_hdemo_sk", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "ss_addr_sk", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "ss_store_sk", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "ss_promo_sk", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "ss_ticket_number", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "ss_quantity", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "ss_wholesale_cost", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
		{Name: "ss_list_price", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
		{Name: "ss_sales_price", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
		{Name: "ss_ext_discount_amt", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
		{Name: "ss_ext_sales_price", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
		{Name: "ss_ext_wholesale_cost", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
		{Name: "ss_ext_list_price", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
		{Name: "ss_ext_tax", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
		{Name: "ss_coupon_amt", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
		{Name: "ss_net_paid", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
		{Name: "ss_net_paid_inc_tax", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
		{Name: "ss_net_profit", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
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

	b0 := builder.Field(0).(*array.Int64Builder)
	b1 := builder.Field(1).(*array.Int64Builder)
	b2 := builder.Field(2).(*array.Int64Builder)
	b3 := builder.Field(3).(*array.Int64Builder)
	b4 := builder.Field(4).(*array.Int64Builder)
	b5 := builder.Field(5).(*array.Int64Builder)
	b6 := builder.Field(6).(*array.Int64Builder)
	b7 := builder.Field(7).(*array.Int64Builder)
	b8 := builder.Field(8).(*array.Int64Builder)
	b9 := builder.Field(9).(*array.Int64Builder)
	b10 := builder.Field(10).(*array.Int32Builder)
	b11 := builder.Field(11).(*array.Float64Builder)
	b12 := builder.Field(12).(*array.Float64Builder)
	b13 := builder.Field(13).(*array.Float64Builder)
	b14 := builder.Field(14).(*array.Float64Builder)
	b15 := builder.Field(15).(*array.Float64Builder)
	b16 := builder.Field(16).(*array.Float64Builder)
	b17 := builder.Field(17).(*array.Float64Builder)
	b18 := builder.Field(18).(*array.Float64Builder)
	b19 := builder.Field(19).(*array.Float64Builder)
	b20 := builder.Field(20).(*array.Float64Builder)
	b21 := builder.Field(21).(*array.Float64Builder)
	b22 := builder.Field(22).(*array.Float64Builder)

	fmt.Printf("Writing %d records to %s (Parquet)...\n", len(rows), targetFilename)

	for i, r := range rows {
		b0.Append(r.SS_SoldDateSK)
		b1.Append(r.SS_SoldTimeSK)
		b2.Append(r.SS_ItemSK)
		b3.Append(r.SS_CustomerSK)
		b4.Append(r.SS_CDemoSK)
		b5.Append(r.SS_HDemoSK)
		b6.Append(r.SS_AddrSK)
		b7.Append(r.SS_StoreSK)
		b8.Append(r.SS_PromoSK)
		b9.Append(r.SS_TicketNumber)
		b10.Append(int32(r.SS_Quantity))
		b11.Append(r.SS_WholesaleCost)
		b12.Append(r.SS_ListPrice)
		b13.Append(r.SS_SalesPrice)
		b14.Append(r.SS_ExtDiscountAmt)
		b15.Append(r.SS_ExtSalesPrice)
		b16.Append(r.SS_ExtWholesaleCost)
		b17.Append(r.SS_ExtListPrice)
		b18.Append(r.SS_ExtTax)
		b19.Append(r.SS_CouponAmt)
		b20.Append(r.SS_NetPaid)
		b21.Append(r.SS_NetPaidIncTax)
		b22.Append(r.SS_NetProfit)

		if (i+1)%typedWriteBatchSize == 0 {
			if err = WriteTypedBatch(writer, builder, targetFilename); err != nil {
				return err
			}
		}
	}

	if len(rows)%typedWriteBatchSize != 0 {
		if err = WriteTypedBatch(writer, builder, targetFilename); err != nil {
			return err
		}
	}

	fmt.Printf("Successfully finished writing to %s\n", targetFilename)
	return nil
}

func WriteCatalogSalesToParquetTyped(rows []ecommerceds.CatalogSales, targetFilename string) (err error) {
	if len(rows) == 0 {
		return nil
	}

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "cs_sold_date_sk", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "cs_sold_time_sk", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "cs_ship_date_sk", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "cs_bill_customer_sk", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "cs_bill_cdemo_sk", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "cs_bill_hdemo_sk", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "cs_bill_addr_sk", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "cs_ship_customer_sk", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "cs_ship_cdemo_sk", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "cs_ship_hdemo_sk", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "cs_ship_addr_sk", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "cs_call_center_sk", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "cs_catalog_page_sk", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "cs_ship_mode_sk", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "cs_warehouse_sk", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "cs_item_sk", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "cs_promo_sk", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "cs_order_number", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "cs_quantity", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "cs_wholesale_cost", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
		{Name: "cs_list_price", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
		{Name: "cs_sales_price", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
		{Name: "cs_ext_discount_amt", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
		{Name: "cs_ext_sales_price", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
		{Name: "cs_ext_wholesale_cost", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
		{Name: "cs_ext_list_price", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
		{Name: "cs_ext_tax", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
		{Name: "cs_coupon_amt", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
		{Name: "cs_ext_ship_cost", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
		{Name: "cs_net_paid", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
		{Name: "cs_net_paid_inc_tax", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
		{Name: "cs_net_paid_inc_ship", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
		{Name: "cs_net_paid_inc_ship_tax", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
		{Name: "cs_net_profit", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
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

	b0 := builder.Field(0).(*array.Int64Builder)
	b1 := builder.Field(1).(*array.Int64Builder)
	b2 := builder.Field(2).(*array.Int64Builder)
	b3 := builder.Field(3).(*array.Int64Builder)
	b4 := builder.Field(4).(*array.Int64Builder)
	b5 := builder.Field(5).(*array.Int64Builder)
	b6 := builder.Field(6).(*array.Int64Builder)
	b7 := builder.Field(7).(*array.Int64Builder)
	b8 := builder.Field(8).(*array.Int64Builder)
	b9 := builder.Field(9).(*array.Int64Builder)
	b10 := builder.Field(10).(*array.Int64Builder)
	b11 := builder.Field(11).(*array.Int64Builder)
	b12 := builder.Field(12).(*array.Int64Builder)
	b13 := builder.Field(13).(*array.Int64Builder)
	b14 := builder.Field(14).(*array.Int64Builder)
	b15 := builder.Field(15).(*array.Int64Builder)
	b16 := builder.Field(16).(*array.Int64Builder)
	b17 := builder.Field(17).(*array.Int64Builder)
	b18 := builder.Field(18).(*array.Int32Builder)
	b19 := builder.Field(19).(*array.Float64Builder)
	b20 := builder.Field(20).(*array.Float64Builder)
	b21 := builder.Field(21).(*array.Float64Builder)
	b22 := builder.Field(22).(*array.Float64Builder)
	b23 := builder.Field(23).(*array.Float64Builder)
	b24 := builder.Field(24).(*array.Float64Builder)
	b25 := builder.Field(25).(*array.Float64Builder)
	b26 := builder.Field(26).(*array.Float64Builder)
	b27 := builder.Field(27).(*array.Float64Builder)
	b28 := builder.Field(28).(*array.Float64Builder)
	b29 := builder.Field(29).(*array.Float64Builder)
	b30 := builder.Field(30).(*array.Float64Builder)
	b31 := builder.Field(31).(*array.Float64Builder)
	b32 := builder.Field(32).(*array.Float64Builder)
	b33 := builder.Field(33).(*array.Float64Builder)

	fmt.Printf("Writing %d records to %s (Parquet)...\n", len(rows), targetFilename)

	for i, r := range rows {
		b0.Append(r.CS_SoldDateSK)
		b1.Append(r.CS_SoldTimeSK)
		b2.Append(r.CS_ShipDateSK)
		b3.Append(r.CS_BillCustomerSK)
		b4.Append(r.CS_BillCDemoSK)
		b5.Append(r.CS_BillHDemoSK)
		b6.Append(r.CS_BillAddrSK)
		b7.Append(r.CS_ShipCustomerSK)
		b8.Append(r.CS_ShipCDemoSK)
		b9.Append(r.CS_ShipHDemoSK)
		b10.Append(r.CS_ShipAddrSK)
		b11.Append(r.CS_CallCenterSK)
		b12.Append(r.CS_CatalogPageSK)
		b13.Append(r.CS_ShipModeSK)
		b14.Append(r.CS_WarehouseSK)
		b15.Append(r.CS_ItemSK)
		b16.Append(r.CS_PromoSK)
		b17.Append(r.CS_OrderNumber)
		b18.Append(int32(r.CS_Quantity))
		b19.Append(r.CS_WholesaleCost)
		b20.Append(r.CS_ListPrice)
		b21.Append(r.CS_SalesPrice)
		b22.Append(r.CS_ExtDiscountAmt)
		b23.Append(r.CS_ExtSalesPrice)
		b24.Append(r.CS_ExtWholesaleCost)
		b25.Append(r.CS_ExtListPrice)
		b26.Append(r.CS_ExtTax)
		b27.Append(r.CS_CouponAmt)
		b28.Append(r.CS_ExtShipCost)
		b29.Append(r.CS_NetPaid)
		b30.Append(r.CS_NetPaidIncTax)
		b31.Append(r.CS_NetPaidIncShip)
		b32.Append(r.CS_NetPaidIncShipTax)
		b33.Append(r.CS_NetProfit)

		if (i+1)%typedWriteBatchSize == 0 {
			if err = WriteTypedBatch(writer, builder, targetFilename); err != nil {
				return err
			}
		}
	}

	if len(rows)%typedWriteBatchSize != 0 {
		if err = WriteTypedBatch(writer, builder, targetFilename); err != nil {
			return err
		}
	}

	fmt.Printf("Successfully finished writing to %s\n", targetFilename)
	return nil
}

func WriteWebSalesToParquetTyped(rows []ecommerceds.WebSales, targetFilename string) (err error) {
	if len(rows) == 0 {
		return nil
	}

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "ws_sold_date_sk", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "ws_sold_time_sk", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "ws_ship_date_sk", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "ws_item_sk", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "ws_bill_customer_sk", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "ws_bill_cdemo_sk", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "ws_bill_hdemo_sk", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "ws_bill_addr_sk", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "ws_ship_customer_sk", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "ws_ship_cdemo_sk", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "ws_ship_hdemo_sk", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "ws_ship_addr_sk", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "ws_web_page_sk", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "ws_web_site_sk", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "ws_ship_mode_sk", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "ws_warehouse_sk", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "ws_promo_sk", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "ws_order_number", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "ws_quantity", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "ws_wholesale_cost", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
		{Name: "ws_list_price", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
		{Name: "ws_sales_price", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
		{Name: "ws_ext_discount_amt", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
		{Name: "ws_ext_sales_price", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
		{Name: "ws_ext_wholesale_cost", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
		{Name: "ws_ext_list_price", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
		{Name: "ws_ext_tax", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
		{Name: "ws_coupon_amt", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
		{Name: "ws_ext_ship_cost", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
		{Name: "ws_net_paid", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
		{Name: "ws_net_paid_inc_tax", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
		{Name: "ws_net_paid_inc_ship", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
		{Name: "ws_net_paid_inc_ship_tax", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
		{Name: "ws_net_profit", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
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

	b0 := builder.Field(0).(*array.Int64Builder)
	b1 := builder.Field(1).(*array.Int64Builder)
	b2 := builder.Field(2).(*array.Int64Builder)
	b3 := builder.Field(3).(*array.Int64Builder)
	b4 := builder.Field(4).(*array.Int64Builder)
	b5 := builder.Field(5).(*array.Int64Builder)
	b6 := builder.Field(6).(*array.Int64Builder)
	b7 := builder.Field(7).(*array.Int64Builder)
	b8 := builder.Field(8).(*array.Int64Builder)
	b9 := builder.Field(9).(*array.Int64Builder)
	b10 := builder.Field(10).(*array.Int64Builder)
	b11 := builder.Field(11).(*array.Int64Builder)
	b12 := builder.Field(12).(*array.Int64Builder)
	b13 := builder.Field(13).(*array.Int64Builder)
	b14 := builder.Field(14).(*array.Int64Builder)
	b15 := builder.Field(15).(*array.Int64Builder)
	b16 := builder.Field(16).(*array.Int64Builder)
	b17 := builder.Field(17).(*array.Int64Builder)
	b18 := builder.Field(18).(*array.Int32Builder)
	b19 := builder.Field(19).(*array.Float64Builder)
	b20 := builder.Field(20).(*array.Float64Builder)
	b21 := builder.Field(21).(*array.Float64Builder)
	b22 := builder.Field(22).(*array.Float64Builder)
	b23 := builder.Field(23).(*array.Float64Builder)
	b24 := builder.Field(24).(*array.Float64Builder)
	b25 := builder.Field(25).(*array.Float64Builder)
	b26 := builder.Field(26).(*array.Float64Builder)
	b27 := builder.Field(27).(*array.Float64Builder)
	b28 := builder.Field(28).(*array.Float64Builder)
	b29 := builder.Field(29).(*array.Float64Builder)
	b30 := builder.Field(30).(*array.Float64Builder)
	b31 := builder.Field(31).(*array.Float64Builder)
	b32 := builder.Field(32).(*array.Float64Builder)
	b33 := builder.Field(33).(*array.Float64Builder)

	fmt.Printf("Writing %d records to %s (Parquet)...\n", len(rows), targetFilename)

	for i, r := range rows {
		b0.Append(r.WS_SoldDateSK)
		b1.Append(r.WS_SoldTimeSK)
		b2.Append(r.WS_ShipDateSK)
		b3.Append(r.WS_ItemSK)
		b4.Append(r.WS_BillCustomerSK)
		b5.Append(r.WS_BillCDemoSK)
		b6.Append(r.WS_BillHDemoSK)
		b7.Append(r.WS_BillAddrSK)
		b8.Append(r.WS_ShipCustomerSK)
		b9.Append(r.WS_ShipCDemoSK)
		b10.Append(r.WS_ShipHDemoSK)
		b11.Append(r.WS_ShipAddrSK)
		b12.Append(r.WS_WebPageSK)
		b13.Append(r.WS_WebSiteSK)
		b14.Append(r.WS_ShipModeSK)
		b15.Append(r.WS_WarehouseSK)
		b16.Append(r.WS_PromoSK)
		b17.Append(r.WS_OrderNumber)
		b18.Append(int32(r.WS_Quantity))
		b19.Append(r.WS_WholesaleCost)
		b20.Append(r.WS_ListPrice)
		b21.Append(r.WS_SalesPrice)
		b22.Append(r.WS_ExtDiscountAmt)
		b23.Append(r.WS_ExtSalesPrice)
		b24.Append(r.WS_ExtWholesaleCost)
		b25.Append(r.WS_ExtListPrice)
		b26.Append(r.WS_ExtTax)
		b27.Append(r.WS_CouponAmt)
		b28.Append(r.WS_ExtShipCost)
		b29.Append(r.WS_NetPaid)
		b30.Append(r.WS_NetPaidIncTax)
		b31.Append(r.WS_NetPaidIncShip)
		b32.Append(r.WS_NetPaidIncShipTax)
		b33.Append(r.WS_NetProfit)

		if (i+1)%typedWriteBatchSize == 0 {
			if err = WriteTypedBatch(writer, builder, targetFilename); err != nil {
				return err
			}
		}
	}

	if len(rows)%typedWriteBatchSize != 0 {
		if err = WriteTypedBatch(writer, builder, targetFilename); err != nil {
			return err
		}
	}

	fmt.Printf("Successfully finished writing to %s\n", targetFilename)
	return nil
}

func WriteDailyStockPricesToParquetTyped(prices []financialmodels.DailyStockPrice, targetFilename string) (err error) {
	if len(prices) == 0 {
		return nil
	}

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "price_id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "date", Type: arrow.FixedWidthTypes.Timestamp_us, Nullable: false},
		{Name: "company_id", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "exchange_id", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "open_price", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
		{Name: "high_price", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
		{Name: "low_price", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
		{Name: "close_price", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
		{Name: "volume", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
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

	b0 := builder.Field(0).(*array.Int64Builder)
	b1 := builder.Field(1).(*array.TimestampBuilder)
	b2 := builder.Field(2).(*array.Int32Builder)
	b3 := builder.Field(3).(*array.Int32Builder)
	b4 := builder.Field(4).(*array.Float64Builder)
	b5 := builder.Field(5).(*array.Float64Builder)
	b6 := builder.Field(6).(*array.Float64Builder)
	b7 := builder.Field(7).(*array.Float64Builder)
	b8 := builder.Field(8).(*array.Int32Builder)

	fmt.Printf("Writing %d records to %s (Parquet)...\n", len(prices), targetFilename)

	for i, p := range prices {
		b0.Append(p.PriceID)
		b1.Append(arrow.Timestamp(p.Date.UnixMicro()))
		b2.Append(int32(p.CompanyID))
		b3.Append(int32(p.ExchangeID))
		b4.Append(p.OpenPrice)
		b5.Append(p.HighPrice)
		b6.Append(p.LowPrice)
		b7.Append(p.ClosePrice)
		b8.Append(int32(p.Volume))

		if (i+1)%typedWriteBatchSize == 0 {
			if err = WriteTypedBatch(writer, builder, targetFilename); err != nil {
				return err
			}
		}
	}

	if len(prices)%typedWriteBatchSize != 0 {
		if err = WriteTypedBatch(writer, builder, targetFilename); err != nil {
			return err
		}
	}

	fmt.Printf("Successfully finished writing to %s\n", targetFilename)
	return nil
}

func WriteAppointmentsToParquetTyped(appts []medicalmodels.Appointment, targetFilename string) (err error) {
	if len(appts) == 0 {
		return nil
	}

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "appointment_id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "patient_id", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "doctor_id", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "clinic_id", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "appointment_date", Type: arrow.FixedWidthTypes.Timestamp_us, Nullable: false},
		{Name: "diagnosis", Type: arrow.BinaryTypes.String, Nullable: false},
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

	b0 := builder.Field(0).(*array.Int64Builder)
	b1 := builder.Field(1).(*array.Int32Builder)
	b2 := builder.Field(2).(*array.Int32Builder)
	b3 := builder.Field(3).(*array.Int32Builder)
	b4 := builder.Field(4).(*array.TimestampBuilder)
	b5 := builder.Field(5).(*array.StringBuilder)

	fmt.Printf("Writing %d records to %s (Parquet)...\n", len(appts), targetFilename)

	for i, a := range appts {
		b0.Append(a.AppointmentID)
		b1.Append(int32(a.PatientID))
		b2.Append(int32(a.DoctorID))
		b3.Append(int32(a.ClinicID))
		b4.Append(arrow.Timestamp(a.AppointmentDate.UnixMicro()))
		b5.Append(a.Diagnosis)

		if (i+1)%typedWriteBatchSize == 0 {
			if err = WriteTypedBatch(writer, builder, targetFilename); err != nil {
				return err
			}
		}
	}

	if len(appts)%typedWriteBatchSize != 0 {
		if err = WriteTypedBatch(writer, builder, targetFilename); err != nil {
			return err
		}
	}

	fmt.Printf("Successfully finished writing to %s\n", targetFilename)
	return nil
}
