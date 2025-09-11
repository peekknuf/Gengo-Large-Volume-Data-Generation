package ecommerceds

import (
	"bufio"
	"fmt"
	"math"
	"math/rand"
	"os"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	ecommerceds "github.com/peekknuf/Gengo/internal/models/ecommerce-ds"
)

// Performance optimization constants
const (
	bufferSize = 64 << 20 // 64MB buffer for NVMe optimization (increased from 32MB)
	flushBatchSize = 50000 // Batch size for periodic flushes (increased from 10K)
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
func generateStoreSalesWorker(count int, startTicket int64, itemSampler, customerSampler, storeSampler, promoSampler *AliasSampler, filename string, rng *rand.Rand) error {
	if count <= 0 {
		return nil
	}

	startTime := time.Now()
	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("failed to create file %s: %w", filename, err)
	}
	defer file.Close()

	writer := bufio.NewWriterSize(file, bufferSize)
	defer writer.Flush()

	// Write header
	header := "ss_sold_date_sk,ss_sold_time_sk,ss_item_sk,ss_customer_sk,ss_cdemo_sk,ss_hdemo_sk,ss_addr_sk,ss_store_sk,ss_promo_sk,ss_ticket_number,ss_quantity,ss_wholesale_cost,ss_list_price,ss_sales_price,ss_ext_discount_amt,ss_ext_sales_price,ss_ext_wholesale_cost,ss_ext_list_price,ss_ext_tax,ss_coupon_amt,ss_net_paid,ss_net_paid_inc_tax,ss_net_profit\n"
	writer.WriteString(header)

	// Pre-calculate common ranges for better performance
	dateSKRange := 2000 // Date SK range (1-2000)
	timeSKRange := 86400 // Time SK range (0-86399)
	cdemoSKRange := 1000 // Customer demo SK range (1-1000)
	hdemoSKRange := 1000 // Household demo SK range (1-1000)
	addrSKRange := 1500 // Address SK range (1-1500)

	// Pre-allocate buffer for row construction (increased to 4KB to reduce allocations)
	rowBuf := make([]byte, 0, 4096)

	for i := 0; i < count; i++ {
		rowBuf = rowBuf[:0] // Reset buffer

		quantity := rng.Intn(10) + 1
		listPriceCents := int64(1000 + rng.Intn(100000)) // 10.00 to 1010.00
		salesPriceCents := listPriceCents * int64(80+rng.Intn(21)) / 100 // 80-100% of list price
		wholesaleCents := salesPriceCents * int64(60+rng.Intn(21)) / 100 // 60-80% of sales price

		// Build CSV row with byte-level formatting using weighted sampling and pre-calculated ranges
		rowBuf = strconv.AppendInt(rowBuf, int64(rng.Intn(dateSKRange)+1), 10) // date_sk
		rowBuf = append(rowBuf, ',')
		rowBuf = strconv.AppendInt(rowBuf, int64(rng.Intn(timeSKRange)), 10) // time_sk
		rowBuf = append(rowBuf, ',')
		rowBuf = strconv.AppendInt(rowBuf, itemSampler.Sample(rng), 10)
		rowBuf = append(rowBuf, ',')
		rowBuf = strconv.AppendInt(rowBuf, customerSampler.Sample(rng), 10)
		rowBuf = append(rowBuf, ',')
		rowBuf = strconv.AppendInt(rowBuf, int64(rng.Intn(cdemoSKRange)+1), 10) // cdemo_sk
		rowBuf = append(rowBuf, ',')
		rowBuf = strconv.AppendInt(rowBuf, int64(rng.Intn(hdemoSKRange)+1), 10) // hdemo_sk
		rowBuf = append(rowBuf, ',')
		rowBuf = strconv.AppendInt(rowBuf, int64(rng.Intn(addrSKRange)+1), 10) // addr_sk
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
		netPaid := extSalesPrice - extDiscountAmt
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

		writer.Write(rowBuf)

		// Periodic flush for better performance
		if (i+1)%flushBatchSize == 0 {
			writer.Flush()
		}
	}

	duration := time.Since(startTime)
	fmt.Printf("Worker completed: %d store sales records to %s in %s\n", count, filename, duration.Round(time.Millisecond))
	return nil
}

// GenerateStoreSalesOptimized generates store sales using worker-based file sharding
func GenerateStoreSalesOptimized(count int, itemSKs, customerSKs, storeSKs, promoSKs []int64, outputDir string) error {
	if count <= 0 {
		return nil
	}

	// Create weighted samplers for key dimensions
	itemSampler, err := NewAliasSampler64(itemSKs)
	if err != nil {
		return fmt.Errorf("failed to create item sampler: %w", err)
	}
	customerSampler, err := NewAliasSampler64(customerSKs)
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

	numWorkers := runtime.NumCPU()
	recordsPerWorker := count / numWorkers
	extraRecords := count % numWorkers

	var wg sync.WaitGroup
	baseSeed := time.Now().UnixNano()
	startTicket := int64(1)

	for i := 0; i < numWorkers; i++ {
		workerRecords := recordsPerWorker
		if i < extraRecords {
			workerRecords++
		}

		if workerRecords > 0 {
			wg.Add(1)
			workerSeed := baseSeed + int64(i)*int64(0x9e3779b9)
			rng := rand.New(rand.NewSource(workerSeed))
			filename := fmt.Sprintf("%s/fact_store_sales_%d.csv", outputDir, i)
			workerStartTicket := startTicket + int64(i*recordsPerWorker)

			go func(records int, ticket int64, fname string, workerRNG *rand.Rand) {
				defer wg.Done()
				if err := generateStoreSalesWorker(records, ticket, itemSampler, customerSampler, storeSampler, promoSampler, fname, workerRNG); err != nil {
					fmt.Printf("Error in store sales worker: %v\n", err)
				}
			}(workerRecords, workerStartTicket, filename, rng)
		}
	}

	wg.Wait()
	return nil
}

// High-performance worker function for generating catalog sales with direct file writing
func generateCatalogSalesWorker(count int, startOrder int64, itemSKs, customerSKs, cdemoSKs, hdemoSKs, addrSKs, callCenterSKs, catalogPageSKs, shipModeSKs, warehouseSKs, promoSKs []int64, filename string, rng *rand.Rand) error {
	if count <= 0 {
		return nil
	}

	startTime := time.Now()
	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("failed to create file %s: %w", filename, err)
	}
	defer file.Close()

	writer := bufio.NewWriterSize(file, bufferSize)
	defer writer.Flush()

	// Write header
	header := "cs_sold_date_sk,cs_sold_time_sk,cs_ship_date_sk,cs_bill_customer_sk,cs_bill_cdemo_sk,cs_bill_hdemo_sk,cs_bill_addr_sk,cs_ship_customer_sk,cs_ship_cdemo_sk,cs_ship_hdemo_sk,cs_ship_addr_sk,cs_call_center_sk,cs_catalog_page_sk,cs_ship_mode_sk,cs_warehouse_sk,cs_item_sk,cs_promo_sk,cs_order_number,cs_quantity,cs_wholesale_cost,cs_list_price,cs_sales_price,cs_ext_discount_amt,cs_ext_sales_price,cs_ext_wholesale_cost,cs_ext_list_price,cs_ext_tax,cs_coupon_amt,cs_ext_ship_cost,cs_net_paid,cs_net_paid_inc_tax,cs_net_paid_inc_ship,cs_net_paid_inc_ship_tax,cs_net_profit\n"
	writer.WriteString(header)

	rowBuf := make([]byte, 0, 4096) // Increased to 4KB to reduce allocations

	for i := 0; i < count; i++ {
		rowBuf = rowBuf[:0]

		quantity := rng.Intn(10) + 1
		listPriceCents := int64(1000 + rng.Intn(100000))
		salesPriceCents := listPriceCents * int64(80+rng.Intn(21)) / 100
		wholesaleCents := salesPriceCents * int64(60+rng.Intn(21)) / 100

		rowBuf = strconv.AppendInt(rowBuf, int64(rng.Intn(2000)+1), 10) // sold_date_sk
		rowBuf = append(rowBuf, ',')
		rowBuf = strconv.AppendInt(rowBuf, int64(rng.Intn(86400)), 10) // sold_time_sk
		rowBuf = append(rowBuf, ',')
		rowBuf = strconv.AppendInt(rowBuf, int64(rng.Intn(2000)+1), 10) // ship_date_sk
		rowBuf = append(rowBuf, ',')
		rowBuf = strconv.AppendInt(rowBuf, customerSKs[rng.Intn(len(customerSKs))], 10)
		rowBuf = append(rowBuf, ',')
		rowBuf = strconv.AppendInt(rowBuf, cdemoSKs[rng.Intn(len(cdemoSKs))], 10)
		rowBuf = append(rowBuf, ',')
		rowBuf = strconv.AppendInt(rowBuf, hdemoSKs[rng.Intn(len(hdemoSKs))], 10)
		rowBuf = append(rowBuf, ',')
		rowBuf = strconv.AppendInt(rowBuf, addrSKs[rng.Intn(len(addrSKs))], 10)
		rowBuf = append(rowBuf, ',')
		rowBuf = strconv.AppendInt(rowBuf, customerSKs[rng.Intn(len(customerSKs))], 10)
		rowBuf = append(rowBuf, ',')
		rowBuf = strconv.AppendInt(rowBuf, cdemoSKs[rng.Intn(len(cdemoSKs))], 10)
		rowBuf = append(rowBuf, ',')
		rowBuf = strconv.AppendInt(rowBuf, hdemoSKs[rng.Intn(len(hdemoSKs))], 10)
		rowBuf = append(rowBuf, ',')
		rowBuf = strconv.AppendInt(rowBuf, addrSKs[rng.Intn(len(addrSKs))], 10)
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
		netPaid := extSalesPrice - extDiscountAmt
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

		writer.Write(rowBuf)

		if (i+1)%flushBatchSize == 0 {
			writer.Flush()
		}
	}

	duration := time.Since(startTime)
	fmt.Printf("Worker completed: %d catalog sales records to %s in %s\n", count, filename, duration.Round(time.Millisecond))
	return nil
}

// GenerateCatalogSalesOptimized generates catalog sales using worker-based file sharding
func GenerateCatalogSalesOptimized(count int, itemSKs, customerSKs, cdemoSKs, hdemoSKs, addrSKs, callCenterSKs, catalogPageSKs, shipModeSKs, warehouseSKs, promoSKs []int64, outputDir string) error {
	if count <= 0 {
		return nil
	}

	numWorkers := runtime.NumCPU()
	recordsPerWorker := count / numWorkers
	extraRecords := count % numWorkers

	var wg sync.WaitGroup
	baseSeed := time.Now().UnixNano()
	startOrder := int64(1)

	for i := 0; i < numWorkers; i++ {
		workerRecords := recordsPerWorker
		if i < extraRecords {
			workerRecords++
		}

		if workerRecords > 0 {
			wg.Add(1)
			workerSeed := baseSeed + int64(i)*int64(0x9e3779b9)
			rng := rand.New(rand.NewSource(workerSeed))
			filename := fmt.Sprintf("%s/fact_catalog_sales_%d.csv", outputDir, i)
			workerStartOrder := startOrder + int64(i*recordsPerWorker)

			go func(records int, order int64, fname string, workerRNG *rand.Rand) {
				defer wg.Done()
				if err := generateCatalogSalesWorker(records, order, itemSKs, customerSKs, cdemoSKs, hdemoSKs, addrSKs, callCenterSKs, catalogPageSKs, shipModeSKs, warehouseSKs, promoSKs, fname, workerRNG); err != nil {
					fmt.Printf("Error in catalog sales worker: %v\n", err)
				}
			}(workerRecords, workerStartOrder, filename, rng)
		}
	}

	wg.Wait()
	return nil
}

// High-performance worker function for generating web sales with direct file writing
func generateWebSalesWorker(count int, startOrder int64, itemSKs, customerSKs, cdemoSKs, hdemoSKs, addrSKs, webPageSKs, webSiteSKs, shipModeSKs, warehouseSKs, promoSKs []int64, filename string, rng *rand.Rand) error {
	if count <= 0 {
		return nil
	}

	startTime := time.Now()
	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("failed to create file %s: %w", filename, err)
	}
	defer file.Close()

	writer := bufio.NewWriterSize(file, bufferSize)
	defer writer.Flush()

	// Write header
	header := "ws_sold_date_sk,ws_sold_time_sk,ws_ship_date_sk,ws_item_sk,ws_bill_customer_sk,ws_bill_cdemo_sk,ws_bill_hdemo_sk,ws_bill_addr_sk,ws_ship_customer_sk,ws_ship_cdemo_sk,ws_ship_hdemo_sk,ws_ship_addr_sk,ws_web_page_sk,ws_web_site_sk,ws_ship_mode_sk,ws_warehouse_sk,ws_promo_sk,ws_order_number,ws_quantity,ws_wholesale_cost,ws_list_price,ws_sales_price,ws_ext_discount_amt,ws_ext_sales_price,ws_ext_wholesale_cost,ws_ext_list_price,ws_ext_tax,ws_coupon_amt,ws_ext_ship_cost,ws_net_paid,ws_net_paid_inc_tax,ws_net_paid_inc_ship,ws_net_paid_inc_ship_tax,ws_net_profit\n"
	writer.WriteString(header)

	rowBuf := make([]byte, 0, 4096) // Increased to 4KB to reduce allocations

	for i := 0; i < count; i++ {
		rowBuf = rowBuf[:0]

		quantity := rng.Intn(10) + 1
		listPriceCents := int64(1000 + rng.Intn(100000))
		salesPriceCents := listPriceCents * int64(80+rng.Intn(21)) / 100
		wholesaleCents := salesPriceCents * int64(60+rng.Intn(21)) / 100

		rowBuf = strconv.AppendInt(rowBuf, int64(rng.Intn(2000)+1), 10) // sold_date_sk
		rowBuf = append(rowBuf, ',')
		rowBuf = strconv.AppendInt(rowBuf, int64(rng.Intn(86400)), 10) // sold_time_sk
		rowBuf = append(rowBuf, ',')
		rowBuf = strconv.AppendInt(rowBuf, int64(rng.Intn(2000)+1), 10) // ship_date_sk
		rowBuf = append(rowBuf, ',')
		rowBuf = strconv.AppendInt(rowBuf, itemSKs[rng.Intn(len(itemSKs))], 10)
		rowBuf = append(rowBuf, ',')
		rowBuf = strconv.AppendInt(rowBuf, customerSKs[rng.Intn(len(customerSKs))], 10)
		rowBuf = append(rowBuf, ',')
		rowBuf = strconv.AppendInt(rowBuf, cdemoSKs[rng.Intn(len(cdemoSKs))], 10)
		rowBuf = append(rowBuf, ',')
		rowBuf = strconv.AppendInt(rowBuf, hdemoSKs[rng.Intn(len(hdemoSKs))], 10)
		rowBuf = append(rowBuf, ',')
		rowBuf = strconv.AppendInt(rowBuf, addrSKs[rng.Intn(len(addrSKs))], 10)
		rowBuf = append(rowBuf, ',')
		rowBuf = strconv.AppendInt(rowBuf, customerSKs[rng.Intn(len(customerSKs))], 10)
		rowBuf = append(rowBuf, ',')
		rowBuf = strconv.AppendInt(rowBuf, cdemoSKs[rng.Intn(len(cdemoSKs))], 10)
		rowBuf = append(rowBuf, ',')
		rowBuf = strconv.AppendInt(rowBuf, hdemoSKs[rng.Intn(len(hdemoSKs))], 10)
		rowBuf = append(rowBuf, ',')
		rowBuf = strconv.AppendInt(rowBuf, addrSKs[rng.Intn(len(addrSKs))], 10)
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
		netPaid := extSalesPrice - extDiscountAmt
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

		writer.Write(rowBuf)

		if (i+1)%flushBatchSize == 0 {
			writer.Flush()
		}
	}

	duration := time.Since(startTime)
	fmt.Printf("Worker completed: %d web sales records to %s in %s\n", count, filename, duration.Round(time.Millisecond))
	return nil
}

// GenerateWebSalesOptimized generates web sales using worker-based file sharding
func GenerateWebSalesOptimized(count int, itemSKs, customerSKs, cdemoSKs, hdemoSKs, addrSKs, webPageSKs, webSiteSKs, shipModeSKs, warehouseSKs, promoSKs []int64, outputDir string) error {
	if count <= 0 {
		return nil
	}

	numWorkers := runtime.NumCPU()
	recordsPerWorker := count / numWorkers
	extraRecords := count % numWorkers

	var wg sync.WaitGroup
	baseSeed := time.Now().UnixNano()
	startOrder := int64(1)

	for i := 0; i < numWorkers; i++ {
		workerRecords := recordsPerWorker
		if i < extraRecords {
			workerRecords++
		}

		if workerRecords > 0 {
			wg.Add(1)
			workerSeed := baseSeed + int64(i)*int64(0x9e3779b9)
			rng := rand.New(rand.NewSource(workerSeed))
			filename := fmt.Sprintf("%s/fact_web_sales_%d.csv", outputDir, i)
			workerStartOrder := startOrder + int64(i*recordsPerWorker)

			go func(records int, order int64, fname string, workerRNG *rand.Rand) {
				defer wg.Done()
				if err := generateWebSalesWorker(records, order, itemSKs, customerSKs, cdemoSKs, hdemoSKs, addrSKs, webPageSKs, webSiteSKs, shipModeSKs, warehouseSKs, promoSKs, fname, workerRNG); err != nil {
					fmt.Printf("Error in web sales worker: %v\n", err)
				}
			}(workerRecords, workerStartOrder, filename, rng)
		}
	}

	wg.Wait()
	return nil
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
			SS_ItemSK:         itemSKs[rand.Intn(len(itemSKs))],
			SS_CustomerSK:     customerSKs[rand.Intn(len(customerSKs))],
			SS_StoreSK:        storeSKs[rand.Intn(len(storeSKs))],
			SS_PromoSK:        promoSKs[rand.Intn(len(promoSKs))],
			SS_TicketNumber:   int64(i + 1),
			SS_Quantity:       quantity,
			SS_ListPrice:      listPrice,
			SS_SalesPrice:     salesPrice,
		}
	}
}