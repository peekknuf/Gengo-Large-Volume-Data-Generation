package ecommerceds

import (
	"bufio"
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"strconv"
	"sync"
	"time"

	ecommerceds "github.com/peekknuf/Gengo/internal/models/ecommerce-ds"
)

// Performance optimization constants
const (
	bufferSize = 32 << 20 // 32MB buffer for NVMe optimization
	flushBatchSize = 10000 // Batch size for periodic flushes
)

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
func generateStoreSalesWorker(count int, startTicket int64, itemSKs, customerSKs, storeSKs, promoSKs []int64, filename string, rng *rand.Rand) error {
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

	// Pre-allocate buffer for row construction
	rowBuf := make([]byte, 0, 512)

	for i := 0; i < count; i++ {
		rowBuf = rowBuf[:0] // Reset buffer

		quantity := rng.Intn(10) + 1
		listPriceCents := int64(1000 + rng.Intn(100000)) // 10.00 to 1010.00
		salesPriceCents := listPriceCents * int64(80+rng.Intn(21)) / 100 // 80-100% of list price
		wholesaleCents := salesPriceCents * int64(60+rng.Intn(21)) / 100 // 60-80% of sales price

		// Build CSV row with byte-level formatting
		rowBuf = strconv.AppendInt(rowBuf, int64(rng.Intn(2000)+1), 10) // date_sk
		rowBuf = append(rowBuf, ',')
		rowBuf = strconv.AppendInt(rowBuf, int64(rng.Intn(86400)), 10) // time_sk
		rowBuf = append(rowBuf, ',')
		rowBuf = strconv.AppendInt(rowBuf, itemSKs[rng.Intn(len(itemSKs))], 10)
		rowBuf = append(rowBuf, ',')
		rowBuf = strconv.AppendInt(rowBuf, customerSKs[rng.Intn(len(customerSKs))], 10)
		rowBuf = append(rowBuf, ',')
		rowBuf = strconv.AppendInt(rowBuf, int64(rng.Intn(1000)+1), 10) // cdemo_sk
		rowBuf = append(rowBuf, ',')
		rowBuf = strconv.AppendInt(rowBuf, int64(rng.Intn(1000)+1), 10) // hdemo_sk
		rowBuf = append(rowBuf, ',')
		rowBuf = strconv.AppendInt(rowBuf, int64(rng.Intn(1500)+1), 10) // addr_sk
		rowBuf = append(rowBuf, ',')
		rowBuf = strconv.AppendInt(rowBuf, storeSKs[rng.Intn(len(storeSKs))], 10)
		rowBuf = append(rowBuf, ',')
		rowBuf = strconv.AppendInt(rowBuf, promoSKs[rng.Intn(len(promoSKs))], 10)
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
				if err := generateStoreSalesWorker(records, ticket, itemSKs, customerSKs, storeSKs, promoSKs, fname, workerRNG); err != nil {
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

	rowBuf := make([]byte, 0, 512)

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

	rowBuf := make([]byte, 0, 512)

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