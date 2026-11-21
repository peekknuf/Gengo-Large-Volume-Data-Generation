package ecommerceds

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"os"
	"strconv"
	"sync/atomic"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/peekknuf/Gengo/internal/formats"
	"github.com/peekknuf/Gengo/internal/generation"
	ecommerceds "github.com/peekknuf/Gengo/internal/models/ecommerce-ds"
)

// SaleDimensions keeps the generated customer relationships and calendar keys
// shared by every retail channel.
type SaleDimensions struct {
	Dates, Times []int64
	Customers    []CustomerKeys
}

type CustomerKeys struct{ ID, Demographics, Household, Address int64 }

func (d SaleDimensions) datesForShipment(rng *rand.Rand) (int64, int64) {
	sold := rng.Intn(len(d.Dates))
	shipped := min(sold+rng.Intn(8), len(d.Dates)-1)
	return d.Dates[sold], d.Dates[shipped]
}

// Performance optimization constants
const (
	bufferSize     = 64 << 20 // 64MB buffer for NVMe optimization (increased from 32MB)
	flushBatchSize = 50000    // Batch size for periodic flushes (increased from 10K)
)

// AliasSampler implements O(1) weighted sampling using Vose's alias method
type AliasSampler struct {
	prob  []float64
	alias []int
	ids   []int64
}

// NewAliasSampler64 builds the alias tables from int64 IDs and implicit weights (1/sqrt(i+1))
func NewAliasSampler64(ids []int64) (*AliasSampler, error) {
	n := len(ids)
	if n == 0 {
		return nil, fmt.Errorf("cannot build sampler from empty ID list")
	}
	if n == 1 {
		// trivial case
		return &AliasSampler{ids: ids, prob: []float64{1.0}, alias: []int{0}}, nil
	}

	// --- 1. Build raw weights ---
	weights := make([]float64, n)
	var total float64
	for i := 0; i < n; i++ {
		// same weighting scheme: 1/sqrt(i+1)
		w := 1.0 / math.Sqrt(float64(i+1))
		weights[i] = w
		total += w
	}

	// normalize
	for i := 0; i < n; i++ {
		weights[i] = weights[i] * float64(n) / total
	}

	// --- 2. Initialize structures ---
	prob := make([]float64, n)
	alias := make([]int, n)

	// two stacks: small and large
	small := make([]int, 0, n)
	large := make([]int, 0, n)

	for i, w := range weights {
		if w < 1.0 {
			small = append(small, i)
		} else {
			large = append(large, i)
		}
	}

	// --- 3. Build tables ---
	for len(small) > 0 && len(large) > 0 {
		s := small[len(small)-1]
		small = small[:len(small)-1]
		l := large[len(large)-1]
		large = large[:len(large)-1]

		prob[s] = weights[s]
		alias[s] = l

		weights[l] = weights[l] + weights[s] - 1.0
		if weights[l] < 1.0 {
			small = append(small, l)
		} else {
			large = append(large, l)
		}
	}

	// whatever remains has prob = 1
	for _, i := range append(small, large...) {
		prob[i] = 1.0
		alias[i] = i
	}

	return &AliasSampler{
		prob:  prob,
		alias: alias,
		ids:   append([]int64(nil), ids...), // copy
	}, nil
}

// Sample draws one ID using O(1) time
func (s *AliasSampler) Sample(rng *rand.Rand) int64 {
	if len(s.ids) == 1 {
		return s.ids[0]
	}
	// pick a column
	i := rng.Intn(len(s.ids))
	// biased coin flip
	if rng.Float64() < s.prob[i] {
		return s.ids[i]
	}
	return s.ids[s.alias[i]]
}

// Block-based ID allocation to reduce atomic contention
const (
	idBlockSize = 100000 // Allocate IDs in blocks of 100K
)

type idBlock struct {
	next int64
	end  int64
}

func (b *idBlock) nextID() int64 {
	if b.next >= b.end {
		return -1 // Block exhausted
	}
	v := b.next
	b.next++
	return v
}

var globalID int64 // Global atomic counter

func getIDBlock() idBlock {
	start := atomicAddInt64(&globalID, idBlockSize) - idBlockSize
	return idBlock{next: start, end: start + idBlockSize}
}

// Atomic add helper for ID block allocation
func atomicAddInt64(addr *int64, delta int64) int64 {
	// For Go 1.19+, we can use sync/atomic directly
	return atomic.AddInt64(addr, delta)
}

// Worker-local ID generator for eliminating atomic contention
type localIDGen struct {
	next int64
	end  int64
}

// appendPrice appends a float-like value with 2 decimal places
// e.g. 1234 -> "12.34"
func appendPrice(buf []byte, cents int64) []byte {
	if cents < 0 {
		buf = append(buf, '-')
		cents = -cents
	}

	intPart := cents / 100
	fracPart := cents % 100

	buf = strconv.AppendInt(buf, intPart, 10)
	buf = append(buf, '.')
	if fracPart < 10 {
		buf = append(buf, '0') // always two digits
	}
	buf = strconv.AppendInt(buf, fracPart, 10)
	return buf
}

// High-performance worker function for generating store sales with direct file writing
func generateStoreSalesWorker(ctx context.Context, count int, startTicket int64, dims SaleDimensions, itemSampler, customerSampler, storeSampler, promoSampler *AliasSampler, filename string, rng *rand.Rand, format string) (err error) {
	if count <= 0 {
		return nil
	}

	if format == "parquet" {
		return generateStoreSalesWorkerParquet(ctx, count, startTicket, dims, itemSampler, customerSampler, storeSampler, promoSampler, filename, rng)
	}
	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("failed to create file %s: %w", filename, err)
	}
	defer func() { err = errors.Join(err, file.Close()) }()

	writer := bufio.NewWriterSize(file, bufferSize)
	defer func() { err = errors.Join(err, writer.Flush()) }()

	// Write header
	header := "ss_sold_date_sk,ss_sold_time_sk,ss_item_sk,ss_customer_sk,ss_cdemo_sk,ss_hdemo_sk,ss_addr_sk,ss_store_sk,ss_promo_sk,ss_ticket_number,ss_quantity,ss_wholesale_cost,ss_list_price,ss_sales_price,ss_ext_discount_amt,ss_ext_sales_price,ss_ext_wholesale_cost,ss_ext_list_price,ss_ext_tax,ss_coupon_amt,ss_net_paid,ss_net_paid_inc_tax,ss_net_profit\n"
	if _, err := writer.WriteString(header); err != nil {
		return err
	}

	// Pre-calculate common ranges for better performance

	// Pre-allocate buffer for row construction (increased to 4KB to reduce allocations)
	rowBuf := make([]byte, 0, 4096)

	for i := 0; i < count; i++ {
		if i%1024 == 0 && ctx.Err() != nil {
			return ctx.Err()
		}
		buyer := dims.Customers[customerSampler.Sample(rng)]
		rowBuf = rowBuf[:0] // Reset buffer

		quantity := rng.Intn(10) + 1
		listPriceCents := int64(1000 + rng.Intn(100000))                 // 10.00 to 1010.00
		salesPriceCents := listPriceCents * int64(80+rng.Intn(21)) / 100 // 80-100% of list price
		wholesaleCents := salesPriceCents * int64(60+rng.Intn(21)) / 100 // 60-80% of sales price

		// Build CSV row with byte-level formatting using weighted sampling and pre-calculated ranges
		rowBuf = strconv.AppendInt(rowBuf, dims.Dates[rng.Intn(len(dims.Dates))], 10) // date_sk
		rowBuf = append(rowBuf, ',')
		rowBuf = strconv.AppendInt(rowBuf, dims.Times[rng.Intn(len(dims.Times))], 10) // time_sk
		rowBuf = append(rowBuf, ',')
		rowBuf = strconv.AppendInt(rowBuf, itemSampler.Sample(rng), 10)
		rowBuf = append(rowBuf, ',')
		rowBuf = strconv.AppendInt(rowBuf, buyer.ID, 10)
		rowBuf = append(rowBuf, ',')
		rowBuf = strconv.AppendInt(rowBuf, buyer.Demographics, 10) // cdemo_sk
		rowBuf = append(rowBuf, ',')
		rowBuf = strconv.AppendInt(rowBuf, buyer.Household, 10) // hdemo_sk
		rowBuf = append(rowBuf, ',')
		rowBuf = strconv.AppendInt(rowBuf, buyer.Address, 10) // addr_sk
		rowBuf = append(rowBuf, ',')
		rowBuf = strconv.AppendInt(rowBuf, storeSampler.Sample(rng), 10)
		rowBuf = append(rowBuf, ',')
		rowBuf = strconv.AppendInt(rowBuf, promoSampler.Sample(rng), 10)
		rowBuf = append(rowBuf, ',')
		rowBuf = strconv.AppendInt(rowBuf, startTicket+int64(i), 10)
		rowBuf = append(rowBuf, ',')
		rowBuf = strconv.AppendInt(rowBuf, int64(quantity), 10)
		rowBuf = append(rowBuf, ',')
		rowBuf = appendPrice(rowBuf, wholesaleCents)
		rowBuf = append(rowBuf, ',')
		rowBuf = appendPrice(rowBuf, listPriceCents)
		rowBuf = append(rowBuf, ',')
		rowBuf = appendPrice(rowBuf, salesPriceCents)
		rowBuf = append(rowBuf, ',')
		// Extended calculations
		extDiscountAmt := (listPriceCents - salesPriceCents) * int64(quantity)
		extSalesPrice := salesPriceCents * int64(quantity)
		extWholesaleCost := wholesaleCents * int64(quantity)
		extListPrice := listPriceCents * int64(quantity)
		extTax := extSalesPrice * 8 / 100 // 8% tax
		couponAmt := int64(0)
		netPaid := extSalesPrice // Unit sales price already includes the discount.
		netPaidIncTax := netPaid + extTax
		netProfit := netPaid - extWholesaleCost

		rowBuf = appendPrice(rowBuf, extDiscountAmt)
		rowBuf = append(rowBuf, ',')
		rowBuf = appendPrice(rowBuf, extSalesPrice)
		rowBuf = append(rowBuf, ',')
		rowBuf = appendPrice(rowBuf, extWholesaleCost)
		rowBuf = append(rowBuf, ',')
		rowBuf = appendPrice(rowBuf, extListPrice)
		rowBuf = append(rowBuf, ',')
		rowBuf = appendPrice(rowBuf, extTax)
		rowBuf = append(rowBuf, ',')
		rowBuf = appendPrice(rowBuf, couponAmt)
		rowBuf = append(rowBuf, ',')
		rowBuf = appendPrice(rowBuf, netPaid)
		rowBuf = append(rowBuf, ',')
		rowBuf = appendPrice(rowBuf, netPaidIncTax)
		rowBuf = append(rowBuf, ',')
		rowBuf = appendPrice(rowBuf, netProfit)
		rowBuf = append(rowBuf, '\n')

		if _, err := writer.Write(rowBuf); err != nil {
			return err
		}

		// Periodic flush for better performance
		if (i+1)%flushBatchSize == 0 {
			if err := writer.Flush(); err != nil {
				return err
			}
		}
	}
	return nil
}

func generateStoreSalesWorkerParquet(ctx context.Context, count int, startTicket int64, dims SaleDimensions, itemSampler, customerSampler, storeSampler, promoSampler *AliasSampler, filename string, rng *rand.Rand) (err error) {

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "ss_sold_date_sk", Type: arrow.PrimitiveTypes.Int64},
		{Name: "ss_sold_time_sk", Type: arrow.PrimitiveTypes.Int64},
		{Name: "ss_item_sk", Type: arrow.PrimitiveTypes.Int64},
		{Name: "ss_customer_sk", Type: arrow.PrimitiveTypes.Int64},
		{Name: "ss_cdemo_sk", Type: arrow.PrimitiveTypes.Int64},
		{Name: "ss_hdemo_sk", Type: arrow.PrimitiveTypes.Int64},
		{Name: "ss_addr_sk", Type: arrow.PrimitiveTypes.Int64},
		{Name: "ss_store_sk", Type: arrow.PrimitiveTypes.Int64},
		{Name: "ss_promo_sk", Type: arrow.PrimitiveTypes.Int64},
		{Name: "ss_ticket_number", Type: arrow.PrimitiveTypes.Int64},
		{Name: "ss_quantity", Type: arrow.PrimitiveTypes.Int32},
		{Name: "ss_wholesale_cost", Type: arrow.PrimitiveTypes.Float64},
		{Name: "ss_list_price", Type: arrow.PrimitiveTypes.Float64},
		{Name: "ss_sales_price", Type: arrow.PrimitiveTypes.Float64},
		{Name: "ss_ext_discount_amt", Type: arrow.PrimitiveTypes.Float64},
		{Name: "ss_ext_sales_price", Type: arrow.PrimitiveTypes.Float64},
		{Name: "ss_ext_wholesale_cost", Type: arrow.PrimitiveTypes.Float64},
		{Name: "ss_ext_list_price", Type: arrow.PrimitiveTypes.Float64},
		{Name: "ss_ext_tax", Type: arrow.PrimitiveTypes.Float64},
		{Name: "ss_coupon_amt", Type: arrow.PrimitiveTypes.Float64},
		{Name: "ss_net_paid", Type: arrow.PrimitiveTypes.Float64},
		{Name: "ss_net_paid_inc_tax", Type: arrow.PrimitiveTypes.Float64},
		{Name: "ss_net_profit", Type: arrow.PrimitiveTypes.Float64},
	}, nil)

	file, writer, builder, err := formats.CreateTypedParquetWriter(schema, filename)
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
			err = fmt.Errorf("error closing parquet writer for %s: %w", filename, closeErr)
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

	for i := 0; i < count; i++ {
		if i%1024 == 0 && ctx.Err() != nil {
			return ctx.Err()
		}
		buyer := dims.Customers[customerSampler.Sample(rng)]
		quantity := rng.Intn(10) + 1
		listPriceCents := int64(1000 + rng.Intn(100000))
		salesPriceCents := listPriceCents * int64(80+rng.Intn(21)) / 100
		wholesaleCents := salesPriceCents * int64(60+rng.Intn(21)) / 100

		extDiscountAmt := (listPriceCents - salesPriceCents) * int64(quantity)
		extSalesPrice := salesPriceCents * int64(quantity)
		extWholesaleCost := wholesaleCents * int64(quantity)
		extListPrice := listPriceCents * int64(quantity)
		extTax := extSalesPrice * 8 / 100
		netPaid := extSalesPrice // Unit sales price already includes the discount.
		netPaidIncTax := netPaid + extTax
		netProfit := netPaid - extWholesaleCost

		b0.Append(dims.Dates[rng.Intn(len(dims.Dates))])
		b1.Append(dims.Times[rng.Intn(len(dims.Times))])
		b2.Append(itemSampler.Sample(rng))
		b3.Append(buyer.ID)
		b4.Append(buyer.Demographics)
		b5.Append(buyer.Household)
		b6.Append(buyer.Address)
		b7.Append(storeSampler.Sample(rng))
		b8.Append(promoSampler.Sample(rng))
		b9.Append(startTicket + int64(i))
		b10.Append(int32(quantity))
		b11.Append(float64(wholesaleCents) / 100.0)
		b12.Append(float64(listPriceCents) / 100.0)
		b13.Append(float64(salesPriceCents) / 100.0)
		b14.Append(float64(extDiscountAmt) / 100.0)
		b15.Append(float64(extSalesPrice) / 100.0)
		b16.Append(float64(extWholesaleCost) / 100.0)
		b17.Append(float64(extListPrice) / 100.0)
		b18.Append(float64(extTax) / 100.0)
		b19.Append(0.0)
		b20.Append(float64(netPaid) / 100.0)
		b21.Append(float64(netPaidIncTax) / 100.0)
		b22.Append(float64(netProfit) / 100.0)

		if (i+1)%65536 == 0 {
			if err = formats.WriteTypedBatch(writer, builder, filename); err != nil {
				return err
			}
		}
	}

	if count%65536 != 0 {
		if err = formats.WriteTypedBatch(writer, builder, filename); err != nil {
			return err
		}
	}
	return nil
}

func writeSaleShards(parent context.Context, count int, dir, channel, format string, write func(context.Context, int, int64, string, *rand.Rand) error, cfg generation.Config) error {
	return generation.Partitions(parent, count, cfg, func(ctx context.Context, p generation.Partition) error {
		rng := cfg.Rand("retail."+channel, uint64(p.Start))
		filename := fmt.Sprintf("%s/fact_%s_sales_%d.%s", dir, channel, p.Index, format)
		if err := write(ctx, p.Count, int64(p.Start)+1, filename, rng); err != nil {
			return err
		}
		return cfg.RecordFile(filename, p.Count, int64(p.Start)+1, int64(p.Start+p.Count))
	})
}

// GenerateStoreSalesOptimized generates store sales using worker-based file sharding
func GenerateStoreSalesOptimized(ctx context.Context, count int, dims SaleDimensions, itemSKs, storeSKs, promoSKs []int64, outputDir string, format string, options ...generation.Config) error {
	cfg := generation.Options(options)
	if count <= 0 {
		return nil
	}

	// Create weighted samplers for key dimensions
	itemSampler, err := NewAliasSampler64(itemSKs)
	if err != nil {
		return fmt.Errorf("failed to create item sampler: %w", err)
	}
	indices := make([]int64, len(dims.Customers))
	for i := range indices {
		indices[i] = int64(i)
	}
	customerSampler, err := NewAliasSampler64(indices)
	if err != nil {
		return fmt.Errorf("failed to create customer sampler: %w", err)
	}
	storeSampler, err := NewAliasSampler64(storeSKs)
	if err != nil {
		return fmt.Errorf("failed to create store sampler: %w", err)
	}
	promoSampler, err := NewAliasSampler64(promoSKs)
	if err != nil {
		return fmt.Errorf("failed to create promo sampler: %w", err)
	}

	return writeSaleShards(ctx, count, outputDir, "store", format, func(ctx context.Context, records int, first int64, filename string, rng *rand.Rand) error {
		return generateStoreSalesWorker(ctx, records, first, dims, itemSampler, customerSampler, storeSampler, promoSampler, filename, rng, format)
	}, cfg)
}

// High-performance worker function for generating catalog sales with direct file writing
func generateCatalogSalesWorker(ctx context.Context, count int, startOrder int64, dims SaleDimensions, itemSKs, callCenterSKs, catalogPageSKs, shipModeSKs, warehouseSKs, promoSKs []int64, filename string, rng *rand.Rand, format string) (err error) {
	if count <= 0 {
		return nil
	}

	if format == "parquet" {
		return generateCatalogSalesWorkerParquet(ctx, count, startOrder, dims, itemSKs, callCenterSKs, catalogPageSKs, shipModeSKs, warehouseSKs, promoSKs, filename, rng)
	}
	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("failed to create file %s: %w", filename, err)
	}
	defer func() { err = errors.Join(err, file.Close()) }()

	writer := bufio.NewWriterSize(file, bufferSize)
	defer func() { err = errors.Join(err, writer.Flush()) }()

	// Write header
	header := "cs_sold_date_sk,cs_sold_time_sk,cs_ship_date_sk,cs_bill_customer_sk,cs_bill_cdemo_sk,cs_bill_hdemo_sk,cs_bill_addr_sk,cs_ship_customer_sk,cs_ship_cdemo_sk,cs_ship_hdemo_sk,cs_ship_addr_sk,cs_call_center_sk,cs_catalog_page_sk,cs_ship_mode_sk,cs_warehouse_sk,cs_item_sk,cs_promo_sk,cs_order_number,cs_quantity,cs_wholesale_cost,cs_list_price,cs_sales_price,cs_ext_discount_amt,cs_ext_sales_price,cs_ext_wholesale_cost,cs_ext_list_price,cs_ext_tax,cs_coupon_amt,cs_ext_ship_cost,cs_net_paid,cs_net_paid_inc_tax,cs_net_paid_inc_ship,cs_net_paid_inc_ship_tax,cs_net_profit\n"
	if _, err := writer.WriteString(header); err != nil {
		return err
	}

	rowBuf := make([]byte, 0, 4096) // Increased to 4KB to reduce allocations

	for i := 0; i < count; i++ {
		if i%1024 == 0 && ctx.Err() != nil {
			return ctx.Err()
		}
		sold, shipped := dims.datesForShipment(rng)
		bill := dims.Customers[rng.Intn(len(dims.Customers))]
		ship := dims.Customers[rng.Intn(len(dims.Customers))]
		rowBuf = rowBuf[:0]

		quantity := rng.Intn(10) + 1
		listPriceCents := int64(1000 + rng.Intn(100000))
		salesPriceCents := listPriceCents * int64(80+rng.Intn(21)) / 100
		wholesaleCents := salesPriceCents * int64(60+rng.Intn(21)) / 100

		rowBuf = strconv.AppendInt(rowBuf, sold, 10) // sold_date_sk
		rowBuf = append(rowBuf, ',')
		rowBuf = strconv.AppendInt(rowBuf, dims.Times[rng.Intn(len(dims.Times))], 10) // sold_time_sk
		rowBuf = append(rowBuf, ',')
		rowBuf = strconv.AppendInt(rowBuf, shipped, 10) // ship_date_sk
		rowBuf = append(rowBuf, ',')
		rowBuf = strconv.AppendInt(rowBuf, bill.ID, 10)
		rowBuf = append(rowBuf, ',')
		rowBuf = strconv.AppendInt(rowBuf, bill.Demographics, 10)
		rowBuf = append(rowBuf, ',')
		rowBuf = strconv.AppendInt(rowBuf, bill.Household, 10)
		rowBuf = append(rowBuf, ',')
		rowBuf = strconv.AppendInt(rowBuf, bill.Address, 10)
		rowBuf = append(rowBuf, ',')
		rowBuf = strconv.AppendInt(rowBuf, ship.ID, 10)
		rowBuf = append(rowBuf, ',')
		rowBuf = strconv.AppendInt(rowBuf, ship.Demographics, 10)
		rowBuf = append(rowBuf, ',')
		rowBuf = strconv.AppendInt(rowBuf, ship.Household, 10)
		rowBuf = append(rowBuf, ',')
		rowBuf = strconv.AppendInt(rowBuf, ship.Address, 10)
		rowBuf = append(rowBuf, ',')
		rowBuf = strconv.AppendInt(rowBuf, callCenterSKs[rng.Intn(len(callCenterSKs))], 10)
		rowBuf = append(rowBuf, ',')
		rowBuf = strconv.AppendInt(rowBuf, catalogPageSKs[rng.Intn(len(catalogPageSKs))], 10)
		rowBuf = append(rowBuf, ',')
		rowBuf = strconv.AppendInt(rowBuf, shipModeSKs[rng.Intn(len(shipModeSKs))], 10)
		rowBuf = append(rowBuf, ',')
		rowBuf = strconv.AppendInt(rowBuf, warehouseSKs[rng.Intn(len(warehouseSKs))], 10)
		rowBuf = append(rowBuf, ',')
		rowBuf = strconv.AppendInt(rowBuf, itemSKs[rng.Intn(len(itemSKs))], 10)
		rowBuf = append(rowBuf, ',')
		rowBuf = strconv.AppendInt(rowBuf, promoSKs[rng.Intn(len(promoSKs))], 10)
		rowBuf = append(rowBuf, ',')
		rowBuf = strconv.AppendInt(rowBuf, startOrder+int64(i), 10)
		rowBuf = append(rowBuf, ',')
		rowBuf = strconv.AppendInt(rowBuf, int64(quantity), 10)
		rowBuf = append(rowBuf, ',')
		rowBuf = appendPrice(rowBuf, wholesaleCents)
		rowBuf = append(rowBuf, ',')
		rowBuf = appendPrice(rowBuf, listPriceCents)
		rowBuf = append(rowBuf, ',')
		rowBuf = appendPrice(rowBuf, salesPriceCents)
		rowBuf = append(rowBuf, ',')

		extDiscountAmt := (listPriceCents - salesPriceCents) * int64(quantity)
		extSalesPrice := salesPriceCents * int64(quantity)
		extWholesaleCost := wholesaleCents * int64(quantity)
		extListPrice := listPriceCents * int64(quantity)
		extTax := extSalesPrice * 8 / 100
		couponAmt := int64(0)
		extShipCost := int64(500 + rng.Intn(2000)) // 5.00 to 25.00
		netPaid := extSalesPrice                   // Unit sales price already includes the discount.
		netPaidIncTax := netPaid + extTax
		netPaidIncShip := netPaid + extShipCost
		netPaidIncShipTax := netPaidIncShip + extTax
		netProfit := netPaid - extWholesaleCost

		rowBuf = appendPrice(rowBuf, extDiscountAmt)
		rowBuf = append(rowBuf, ',')
		rowBuf = appendPrice(rowBuf, extSalesPrice)
		rowBuf = append(rowBuf, ',')
		rowBuf = appendPrice(rowBuf, extWholesaleCost)
		rowBuf = append(rowBuf, ',')
		rowBuf = appendPrice(rowBuf, extListPrice)
		rowBuf = append(rowBuf, ',')
		rowBuf = appendPrice(rowBuf, extTax)
		rowBuf = append(rowBuf, ',')
		rowBuf = appendPrice(rowBuf, couponAmt)
		rowBuf = append(rowBuf, ',')
		rowBuf = appendPrice(rowBuf, extShipCost)
		rowBuf = append(rowBuf, ',')
		rowBuf = appendPrice(rowBuf, netPaid)
		rowBuf = append(rowBuf, ',')
		rowBuf = appendPrice(rowBuf, netPaidIncTax)
		rowBuf = append(rowBuf, ',')
		rowBuf = appendPrice(rowBuf, netPaidIncShip)
		rowBuf = append(rowBuf, ',')
		rowBuf = appendPrice(rowBuf, netPaidIncShipTax)
		rowBuf = append(rowBuf, ',')
		rowBuf = appendPrice(rowBuf, netProfit)
		rowBuf = append(rowBuf, '\n')

		if _, err := writer.Write(rowBuf); err != nil {
			return err
		}

		if (i+1)%flushBatchSize == 0 {
			if err := writer.Flush(); err != nil {
				return err
			}
		}
	}
	return nil
}

func generateCatalogSalesWorkerParquet(ctx context.Context, count int, startOrder int64, dims SaleDimensions, itemSKs, callCenterSKs, catalogPageSKs, shipModeSKs, warehouseSKs, promoSKs []int64, filename string, rng *rand.Rand) (err error) {

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "cs_sold_date_sk", Type: arrow.PrimitiveTypes.Int64},
		{Name: "cs_sold_time_sk", Type: arrow.PrimitiveTypes.Int64},
		{Name: "cs_ship_date_sk", Type: arrow.PrimitiveTypes.Int64},
		{Name: "cs_bill_customer_sk", Type: arrow.PrimitiveTypes.Int64},
		{Name: "cs_bill_cdemo_sk", Type: arrow.PrimitiveTypes.Int64},
		{Name: "cs_bill_hdemo_sk", Type: arrow.PrimitiveTypes.Int64},
		{Name: "cs_bill_addr_sk", Type: arrow.PrimitiveTypes.Int64},
		{Name: "cs_ship_customer_sk", Type: arrow.PrimitiveTypes.Int64},
		{Name: "cs_ship_cdemo_sk", Type: arrow.PrimitiveTypes.Int64},
		{Name: "cs_ship_hdemo_sk", Type: arrow.PrimitiveTypes.Int64},
		{Name: "cs_ship_addr_sk", Type: arrow.PrimitiveTypes.Int64},
		{Name: "cs_call_center_sk", Type: arrow.PrimitiveTypes.Int64},
		{Name: "cs_catalog_page_sk", Type: arrow.PrimitiveTypes.Int64},
		{Name: "cs_ship_mode_sk", Type: arrow.PrimitiveTypes.Int64},
		{Name: "cs_warehouse_sk", Type: arrow.PrimitiveTypes.Int64},
		{Name: "cs_item_sk", Type: arrow.PrimitiveTypes.Int64},
		{Name: "cs_promo_sk", Type: arrow.PrimitiveTypes.Int64},
		{Name: "cs_order_number", Type: arrow.PrimitiveTypes.Int64},
		{Name: "cs_quantity", Type: arrow.PrimitiveTypes.Int32},
		{Name: "cs_wholesale_cost", Type: arrow.PrimitiveTypes.Float64},
		{Name: "cs_list_price", Type: arrow.PrimitiveTypes.Float64},
		{Name: "cs_sales_price", Type: arrow.PrimitiveTypes.Float64},
		{Name: "cs_ext_discount_amt", Type: arrow.PrimitiveTypes.Float64},
		{Name: "cs_ext_sales_price", Type: arrow.PrimitiveTypes.Float64},
		{Name: "cs_ext_wholesale_cost", Type: arrow.PrimitiveTypes.Float64},
		{Name: "cs_ext_list_price", Type: arrow.PrimitiveTypes.Float64},
		{Name: "cs_ext_tax", Type: arrow.PrimitiveTypes.Float64},
		{Name: "cs_coupon_amt", Type: arrow.PrimitiveTypes.Float64},
		{Name: "cs_ext_ship_cost", Type: arrow.PrimitiveTypes.Float64},
		{Name: "cs_net_paid", Type: arrow.PrimitiveTypes.Float64},
		{Name: "cs_net_paid_inc_tax", Type: arrow.PrimitiveTypes.Float64},
		{Name: "cs_net_paid_inc_ship", Type: arrow.PrimitiveTypes.Float64},
		{Name: "cs_net_paid_inc_ship_tax", Type: arrow.PrimitiveTypes.Float64},
		{Name: "cs_net_profit", Type: arrow.PrimitiveTypes.Float64},
	}, nil)

	file, writer, builder, err := formats.CreateTypedParquetWriter(schema, filename)
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
			err = fmt.Errorf("error closing parquet writer for %s: %w", filename, closeErr)
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

	for i := 0; i < count; i++ {
		if i%1024 == 0 && ctx.Err() != nil {
			return ctx.Err()
		}
		sold, shipped := dims.datesForShipment(rng)
		bill := dims.Customers[rng.Intn(len(dims.Customers))]
		ship := dims.Customers[rng.Intn(len(dims.Customers))]
		quantity := rng.Intn(10) + 1
		listPriceCents := int64(1000 + rng.Intn(100000))
		salesPriceCents := listPriceCents * int64(80+rng.Intn(21)) / 100
		wholesaleCents := salesPriceCents * int64(60+rng.Intn(21)) / 100

		extDiscountAmt := (listPriceCents - salesPriceCents) * int64(quantity)
		extSalesPrice := salesPriceCents * int64(quantity)
		extWholesaleCost := wholesaleCents * int64(quantity)
		extListPrice := listPriceCents * int64(quantity)
		extTax := extSalesPrice * 8 / 100
		key1 := dims.Times[rng.Intn(len(dims.Times))]
		key11 := callCenterSKs[rng.Intn(len(callCenterSKs))]
		key12 := catalogPageSKs[rng.Intn(len(catalogPageSKs))]
		key13 := shipModeSKs[rng.Intn(len(shipModeSKs))]
		key14 := warehouseSKs[rng.Intn(len(warehouseSKs))]
		key15 := itemSKs[rng.Intn(len(itemSKs))]
		key16 := promoSKs[rng.Intn(len(promoSKs))]
		extShipCost := int64(500 + rng.Intn(2000))
		netPaid := extSalesPrice // Unit sales price already includes the discount.
		netPaidIncTax := netPaid + extTax
		netPaidIncShip := netPaid + extShipCost
		netPaidIncShipTax := netPaidIncShip + extTax
		netProfit := netPaid - extWholesaleCost

		b0.Append(sold)
		b1.Append(key1)
		b2.Append(shipped)
		b3.Append(bill.ID)
		b4.Append(bill.Demographics)
		b5.Append(bill.Household)
		b6.Append(bill.Address)
		b7.Append(ship.ID)
		b8.Append(ship.Demographics)
		b9.Append(ship.Household)
		b10.Append(ship.Address)
		b11.Append(key11)
		b12.Append(key12)
		b13.Append(key13)
		b14.Append(key14)
		b15.Append(key15)
		b16.Append(key16)
		b17.Append(startOrder + int64(i))
		b18.Append(int32(quantity))
		b19.Append(float64(wholesaleCents) / 100.0)
		b20.Append(float64(listPriceCents) / 100.0)
		b21.Append(float64(salesPriceCents) / 100.0)
		b22.Append(float64(extDiscountAmt) / 100.0)
		b23.Append(float64(extSalesPrice) / 100.0)
		b24.Append(float64(extWholesaleCost) / 100.0)
		b25.Append(float64(extListPrice) / 100.0)
		b26.Append(float64(extTax) / 100.0)
		b27.Append(0.0)
		b28.Append(float64(extShipCost) / 100.0)
		b29.Append(float64(netPaid) / 100.0)
		b30.Append(float64(netPaidIncTax) / 100.0)
		b31.Append(float64(netPaidIncShip) / 100.0)
		b32.Append(float64(netPaidIncShipTax) / 100.0)
		b33.Append(float64(netProfit) / 100.0)

		if (i+1)%65536 == 0 {
			if err = formats.WriteTypedBatch(writer, builder, filename); err != nil {
				return err
			}
		}
	}

	if count%65536 != 0 {
		if err = formats.WriteTypedBatch(writer, builder, filename); err != nil {
			return err
		}
	}
	return nil
}

// GenerateCatalogSalesOptimized generates catalog sales using worker-based file sharding
func GenerateCatalogSalesOptimized(ctx context.Context, count int, dims SaleDimensions, itemSKs, callCenterSKs, catalogPageSKs, shipModeSKs, warehouseSKs, promoSKs []int64, outputDir string, format string, options ...generation.Config) error {
	cfg := generation.Options(options)
	if count <= 0 {
		return nil
	}

	return writeSaleShards(ctx, count, outputDir, "catalog", format, func(ctx context.Context, records int, first int64, filename string, rng *rand.Rand) error {
		return generateCatalogSalesWorker(ctx, records, first, dims, itemSKs, callCenterSKs, catalogPageSKs, shipModeSKs, warehouseSKs, promoSKs, filename, rng, format)
	}, cfg)
}

// High-performance worker function for generating web sales with direct file writing
func generateWebSalesWorker(ctx context.Context, count int, startOrder int64, dims SaleDimensions, itemSKs, webPageSKs, webSiteSKs, shipModeSKs, warehouseSKs, promoSKs []int64, filename string, rng *rand.Rand, format string) (err error) {
	if count <= 0 {
		return nil
	}

	if format == "parquet" {
		return generateWebSalesWorkerParquet(ctx, count, startOrder, dims, itemSKs, webPageSKs, webSiteSKs, shipModeSKs, warehouseSKs, promoSKs, filename, rng)
	}
	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("failed to create file %s: %w", filename, err)
	}
	defer func() { err = errors.Join(err, file.Close()) }()

	writer := bufio.NewWriterSize(file, bufferSize)
	defer func() { err = errors.Join(err, writer.Flush()) }()

	// Write header
	header := "ws_sold_date_sk,ws_sold_time_sk,ws_ship_date_sk,ws_item_sk,ws_bill_customer_sk,ws_bill_cdemo_sk,ws_bill_hdemo_sk,ws_bill_addr_sk,ws_ship_customer_sk,ws_ship_cdemo_sk,ws_ship_hdemo_sk,ws_ship_addr_sk,ws_web_page_sk,ws_web_site_sk,ws_ship_mode_sk,ws_warehouse_sk,ws_promo_sk,ws_order_number,ws_quantity,ws_wholesale_cost,ws_list_price,ws_sales_price,ws_ext_discount_amt,ws_ext_sales_price,ws_ext_wholesale_cost,ws_ext_list_price,ws_ext_tax,ws_coupon_amt,ws_ext_ship_cost,ws_net_paid,ws_net_paid_inc_tax,ws_net_paid_inc_ship,ws_net_paid_inc_ship_tax,ws_net_profit\n"
	if _, err := writer.WriteString(header); err != nil {
		return err
	}

	rowBuf := make([]byte, 0, 4096) // Increased to 4KB to reduce allocations

	for i := 0; i < count; i++ {
		if i%1024 == 0 && ctx.Err() != nil {
			return ctx.Err()
		}
		sold, shipped := dims.datesForShipment(rng)
		bill := dims.Customers[rng.Intn(len(dims.Customers))]
		ship := dims.Customers[rng.Intn(len(dims.Customers))]
		rowBuf = rowBuf[:0]

		quantity := rng.Intn(10) + 1
		listPriceCents := int64(1000 + rng.Intn(100000))
		salesPriceCents := listPriceCents * int64(80+rng.Intn(21)) / 100
		wholesaleCents := salesPriceCents * int64(60+rng.Intn(21)) / 100

		rowBuf = strconv.AppendInt(rowBuf, sold, 10) // sold_date_sk
		rowBuf = append(rowBuf, ',')
		rowBuf = strconv.AppendInt(rowBuf, dims.Times[rng.Intn(len(dims.Times))], 10) // sold_time_sk
		rowBuf = append(rowBuf, ',')
		rowBuf = strconv.AppendInt(rowBuf, shipped, 10) // ship_date_sk
		rowBuf = append(rowBuf, ',')
		rowBuf = strconv.AppendInt(rowBuf, itemSKs[rng.Intn(len(itemSKs))], 10)
		rowBuf = append(rowBuf, ',')
		rowBuf = strconv.AppendInt(rowBuf, bill.ID, 10)
		rowBuf = append(rowBuf, ',')
		rowBuf = strconv.AppendInt(rowBuf, bill.Demographics, 10)
		rowBuf = append(rowBuf, ',')
		rowBuf = strconv.AppendInt(rowBuf, bill.Household, 10)
		rowBuf = append(rowBuf, ',')
		rowBuf = strconv.AppendInt(rowBuf, bill.Address, 10)
		rowBuf = append(rowBuf, ',')
		rowBuf = strconv.AppendInt(rowBuf, ship.ID, 10)
		rowBuf = append(rowBuf, ',')
		rowBuf = strconv.AppendInt(rowBuf, ship.Demographics, 10)
		rowBuf = append(rowBuf, ',')
		rowBuf = strconv.AppendInt(rowBuf, ship.Household, 10)
		rowBuf = append(rowBuf, ',')
		rowBuf = strconv.AppendInt(rowBuf, ship.Address, 10)
		rowBuf = append(rowBuf, ',')
		rowBuf = strconv.AppendInt(rowBuf, webPageSKs[rng.Intn(len(webPageSKs))], 10)
		rowBuf = append(rowBuf, ',')
		rowBuf = strconv.AppendInt(rowBuf, webSiteSKs[rng.Intn(len(webSiteSKs))], 10)
		rowBuf = append(rowBuf, ',')
		rowBuf = strconv.AppendInt(rowBuf, shipModeSKs[rng.Intn(len(shipModeSKs))], 10)
		rowBuf = append(rowBuf, ',')
		rowBuf = strconv.AppendInt(rowBuf, warehouseSKs[rng.Intn(len(warehouseSKs))], 10)
		rowBuf = append(rowBuf, ',')
		rowBuf = strconv.AppendInt(rowBuf, promoSKs[rng.Intn(len(promoSKs))], 10)
		rowBuf = append(rowBuf, ',')
		rowBuf = strconv.AppendInt(rowBuf, startOrder+int64(i), 10)
		rowBuf = append(rowBuf, ',')
		rowBuf = strconv.AppendInt(rowBuf, int64(quantity), 10)
		rowBuf = append(rowBuf, ',')
		rowBuf = appendPrice(rowBuf, wholesaleCents)
		rowBuf = append(rowBuf, ',')
		rowBuf = appendPrice(rowBuf, listPriceCents)
		rowBuf = append(rowBuf, ',')
		rowBuf = appendPrice(rowBuf, salesPriceCents)
		rowBuf = append(rowBuf, ',')

		extDiscountAmt := (listPriceCents - salesPriceCents) * int64(quantity)
		extSalesPrice := salesPriceCents * int64(quantity)
		extWholesaleCost := wholesaleCents * int64(quantity)
		extListPrice := listPriceCents * int64(quantity)
		extTax := extSalesPrice * 8 / 100
		couponAmt := int64(0)
		extShipCost := int64(500 + rng.Intn(2000))
		netPaid := extSalesPrice // Unit sales price already includes the discount.
		netPaidIncTax := netPaid + extTax
		netPaidIncShip := netPaid + extShipCost
		netPaidIncShipTax := netPaidIncShip + extTax
		netProfit := netPaid - extWholesaleCost

		rowBuf = appendPrice(rowBuf, extDiscountAmt)
		rowBuf = append(rowBuf, ',')
		rowBuf = appendPrice(rowBuf, extSalesPrice)
		rowBuf = append(rowBuf, ',')
		rowBuf = appendPrice(rowBuf, extWholesaleCost)
		rowBuf = append(rowBuf, ',')
		rowBuf = appendPrice(rowBuf, extListPrice)
		rowBuf = append(rowBuf, ',')
		rowBuf = appendPrice(rowBuf, extTax)
		rowBuf = append(rowBuf, ',')
		rowBuf = appendPrice(rowBuf, couponAmt)
		rowBuf = append(rowBuf, ',')
		rowBuf = appendPrice(rowBuf, extShipCost)
		rowBuf = append(rowBuf, ',')
		rowBuf = appendPrice(rowBuf, netPaid)
		rowBuf = append(rowBuf, ',')
		rowBuf = appendPrice(rowBuf, netPaidIncTax)
		rowBuf = append(rowBuf, ',')
		rowBuf = appendPrice(rowBuf, netPaidIncShip)
		rowBuf = append(rowBuf, ',')
		rowBuf = appendPrice(rowBuf, netPaidIncShipTax)
		rowBuf = append(rowBuf, ',')
		rowBuf = appendPrice(rowBuf, netProfit)
		rowBuf = append(rowBuf, '\n')

		if _, err := writer.Write(rowBuf); err != nil {
			return err
		}

		if (i+1)%flushBatchSize == 0 {
			if err := writer.Flush(); err != nil {
				return err
			}
		}
	}
	return nil
}

func generateWebSalesWorkerParquet(ctx context.Context, count int, startOrder int64, dims SaleDimensions, itemSKs, webPageSKs, webSiteSKs, shipModeSKs, warehouseSKs, promoSKs []int64, filename string, rng *rand.Rand) (err error) {

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "ws_sold_date_sk", Type: arrow.PrimitiveTypes.Int64},
		{Name: "ws_sold_time_sk", Type: arrow.PrimitiveTypes.Int64},
		{Name: "ws_ship_date_sk", Type: arrow.PrimitiveTypes.Int64},
		{Name: "ws_item_sk", Type: arrow.PrimitiveTypes.Int64},
		{Name: "ws_bill_customer_sk", Type: arrow.PrimitiveTypes.Int64},
		{Name: "ws_bill_cdemo_sk", Type: arrow.PrimitiveTypes.Int64},
		{Name: "ws_bill_hdemo_sk", Type: arrow.PrimitiveTypes.Int64},
		{Name: "ws_bill_addr_sk", Type: arrow.PrimitiveTypes.Int64},
		{Name: "ws_ship_customer_sk", Type: arrow.PrimitiveTypes.Int64},
		{Name: "ws_ship_cdemo_sk", Type: arrow.PrimitiveTypes.Int64},
		{Name: "ws_ship_hdemo_sk", Type: arrow.PrimitiveTypes.Int64},
		{Name: "ws_ship_addr_sk", Type: arrow.PrimitiveTypes.Int64},
		{Name: "ws_web_page_sk", Type: arrow.PrimitiveTypes.Int64},
		{Name: "ws_web_site_sk", Type: arrow.PrimitiveTypes.Int64},
		{Name: "ws_ship_mode_sk", Type: arrow.PrimitiveTypes.Int64},
		{Name: "ws_warehouse_sk", Type: arrow.PrimitiveTypes.Int64},
		{Name: "ws_promo_sk", Type: arrow.PrimitiveTypes.Int64},
		{Name: "ws_order_number", Type: arrow.PrimitiveTypes.Int64},
		{Name: "ws_quantity", Type: arrow.PrimitiveTypes.Int32},
		{Name: "ws_wholesale_cost", Type: arrow.PrimitiveTypes.Float64},
		{Name: "ws_list_price", Type: arrow.PrimitiveTypes.Float64},
		{Name: "ws_sales_price", Type: arrow.PrimitiveTypes.Float64},
		{Name: "ws_ext_discount_amt", Type: arrow.PrimitiveTypes.Float64},
		{Name: "ws_ext_sales_price", Type: arrow.PrimitiveTypes.Float64},
		{Name: "ws_ext_wholesale_cost", Type: arrow.PrimitiveTypes.Float64},
		{Name: "ws_ext_list_price", Type: arrow.PrimitiveTypes.Float64},
		{Name: "ws_ext_tax", Type: arrow.PrimitiveTypes.Float64},
		{Name: "ws_coupon_amt", Type: arrow.PrimitiveTypes.Float64},
		{Name: "ws_ext_ship_cost", Type: arrow.PrimitiveTypes.Float64},
		{Name: "ws_net_paid", Type: arrow.PrimitiveTypes.Float64},
		{Name: "ws_net_paid_inc_tax", Type: arrow.PrimitiveTypes.Float64},
		{Name: "ws_net_paid_inc_ship", Type: arrow.PrimitiveTypes.Float64},
		{Name: "ws_net_paid_inc_ship_tax", Type: arrow.PrimitiveTypes.Float64},
		{Name: "ws_net_profit", Type: arrow.PrimitiveTypes.Float64},
	}, nil)

	file, writer, builder, err := formats.CreateTypedParquetWriter(schema, filename)
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
			err = fmt.Errorf("error closing parquet writer for %s: %w", filename, closeErr)
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

	for i := 0; i < count; i++ {
		if i%1024 == 0 && ctx.Err() != nil {
			return ctx.Err()
		}
		sold, shipped := dims.datesForShipment(rng)
		bill := dims.Customers[rng.Intn(len(dims.Customers))]
		ship := dims.Customers[rng.Intn(len(dims.Customers))]
		quantity := rng.Intn(10) + 1
		listPriceCents := int64(1000 + rng.Intn(100000))
		salesPriceCents := listPriceCents * int64(80+rng.Intn(21)) / 100
		wholesaleCents := salesPriceCents * int64(60+rng.Intn(21)) / 100

		extDiscountAmt := (listPriceCents - salesPriceCents) * int64(quantity)
		extSalesPrice := salesPriceCents * int64(quantity)
		extWholesaleCost := wholesaleCents * int64(quantity)
		extListPrice := listPriceCents * int64(quantity)
		extTax := extSalesPrice * 8 / 100
		key1 := dims.Times[rng.Intn(len(dims.Times))]
		key3 := itemSKs[rng.Intn(len(itemSKs))]
		key12 := webPageSKs[rng.Intn(len(webPageSKs))]
		key13 := webSiteSKs[rng.Intn(len(webSiteSKs))]
		key14 := shipModeSKs[rng.Intn(len(shipModeSKs))]
		key15 := warehouseSKs[rng.Intn(len(warehouseSKs))]
		key16 := promoSKs[rng.Intn(len(promoSKs))]
		extShipCost := int64(500 + rng.Intn(2000))
		netPaid := extSalesPrice // Unit sales price already includes the discount.
		netPaidIncTax := netPaid + extTax
		netPaidIncShip := netPaid + extShipCost
		netPaidIncShipTax := netPaidIncShip + extTax
		netProfit := netPaid - extWholesaleCost

		b0.Append(sold)
		b1.Append(key1)
		b2.Append(shipped)
		b3.Append(key3)
		b4.Append(bill.ID)
		b5.Append(bill.Demographics)
		b6.Append(bill.Household)
		b7.Append(bill.Address)
		b8.Append(ship.ID)
		b9.Append(ship.Demographics)
		b10.Append(ship.Household)
		b11.Append(ship.Address)
		b12.Append(key12)
		b13.Append(key13)
		b14.Append(key14)
		b15.Append(key15)
		b16.Append(key16)
		b17.Append(startOrder + int64(i))
		b18.Append(int32(quantity))
		b19.Append(float64(wholesaleCents) / 100.0)
		b20.Append(float64(listPriceCents) / 100.0)
		b21.Append(float64(salesPriceCents) / 100.0)
		b22.Append(float64(extDiscountAmt) / 100.0)
		b23.Append(float64(extSalesPrice) / 100.0)
		b24.Append(float64(extWholesaleCost) / 100.0)
		b25.Append(float64(extListPrice) / 100.0)
		b26.Append(float64(extTax) / 100.0)
		b27.Append(0.0)
		b28.Append(float64(extShipCost) / 100.0)
		b29.Append(float64(netPaid) / 100.0)
		b30.Append(float64(netPaidIncTax) / 100.0)
		b31.Append(float64(netPaidIncShip) / 100.0)
		b32.Append(float64(netPaidIncShipTax) / 100.0)
		b33.Append(float64(netProfit) / 100.0)

		if (i+1)%65536 == 0 {
			if err = formats.WriteTypedBatch(writer, builder, filename); err != nil {
				return err
			}
		}
	}

	if count%65536 != 0 {
		if err = formats.WriteTypedBatch(writer, builder, filename); err != nil {
			return err
		}
	}
	return nil
}

// GenerateWebSalesOptimized generates web sales using worker-based file sharding
func GenerateWebSalesOptimized(ctx context.Context, count int, dims SaleDimensions, itemSKs, webPageSKs, webSiteSKs, shipModeSKs, warehouseSKs, promoSKs []int64, outputDir string, format string, options ...generation.Config) error {
	cfg := generation.Options(options)
	if count <= 0 {
		return nil
	}

	return writeSaleShards(ctx, count, outputDir, "web", format, func(ctx context.Context, records int, first int64, filename string, rng *rand.Rand) error {
		return generateWebSalesWorker(ctx, records, first, dims, itemSKs, webPageSKs, webSiteSKs, shipModeSKs, warehouseSKs, promoSKs, filename, rng, format)
	}, cfg)
}

// Legacy channel-based functions - kept for backward compatibility but not used in optimized flow

// GenerateStoreSales generates a number of store sales records.
func GenerateStoreSales(count int, itemSKs, customerSKs, storeSKs, promoSKs []int64, ch chan<- interface{}) {
	// This function is kept for backward compatibility but replaced by GenerateStoreSalesOptimized
	for i := 0; i < count; i++ {
		quantity := rand.Intn(10) + 1
		listPrice := 10.0 + rand.Float64()*1000
		salesPrice := listPrice * (1 - rand.Float64()*0.2)

		ch <- ecommerceds.StoreSales{
			SS_ItemSK:       itemSKs[rand.Intn(len(itemSKs))],
			SS_CustomerSK:   customerSKs[rand.Intn(len(customerSKs))],
			SS_StoreSK:      storeSKs[rand.Intn(len(storeSKs))],
			SS_PromoSK:      promoSKs[rand.Intn(len(promoSKs))],
			SS_TicketNumber: int64(i + 1),
			SS_Quantity:     quantity,
			SS_ListPrice:    listPrice,
			SS_SalesPrice:   salesPrice,
		}
	}
}
