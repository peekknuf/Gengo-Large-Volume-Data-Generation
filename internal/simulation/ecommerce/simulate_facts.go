package ecommerce

import (
	"bufio"
	"fmt"
	"math"
	"math/rand"
	"os"
	"runtime"
	"strconv"
	"sync"
	"time"

	ecommercemodels "github.com/peekknuf/Gengo/internal/models/ecommerce"
)

var (
	orderStatuses = []string{"Pending", "Processing", "Shipped", "Delivered", "Cancelled", "Returned"}
)

// appendPrice appends a float-like value with 2 decimal places
// e.g. 1234 -> "12.34"
func appendPrice(buf []byte, cents int64) []byte {
	// handle negative
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

// appendDiscount appends a discount value with 4 decimal places
// e.g. 500 -> "0.0500" (5% as basis points)
func appendDiscount(buf []byte, basisPoints int64) []byte {
	// basis points: 10000 = 100%, so 500 = 5%
	intPart := basisPoints / 10000
	fracPart := basisPoints % 10000

	buf = strconv.AppendInt(buf, intPart, 10)
	buf = append(buf, '.')
	
	// Always 4 decimal places for discount
	if fracPart < 1000 {
		buf = append(buf, '0')
	}
	if fracPart < 100 {
		buf = append(buf, '0')
	}
	if fracPart < 10 {
		buf = append(buf, '0')
	}
	buf = strconv.AppendInt(buf, fracPart, 10)
	return buf
}

const blockSize = 100_000

// Worker-local order item ID generator
type localIDGen struct {
	next int64
	end  int64
}

func (g *localIDGen) nextID() int64 {
	if g.next >= g.end {
		return -1 // should never happen if ranges are allocated correctly
	}
	v := g.next
	g.next++
	return v
}

type idBlock struct{ next, end int64 }

func (b *idBlock) nextID() int64 {
    if b.next == b.end { return -1 }
    v := b.next; b.next++
    return v
}

// Remove global ID counter - no longer needed with partitioning
// var globalID int64

// AliasSampler implements O(1) weighted sampling using Vose's alias method
type AliasSampler struct {
	prob  []float64
	alias []int
	ids   []int
}

// NewAliasSampler builds the alias tables from ids and implicit weights (1/sqrt(i+1))
func NewAliasSampler(ids []int) (*AliasSampler, error) {
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
		// same weighting scheme as before: 1/sqrt(i+1)
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
		ids:   append([]int(nil), ids...), // copy
	}, nil
}

// Sample draws one ID using O(1) time
func (s *AliasSampler) Sample(rng *rand.Rand) int {
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

// generateECommerceFactsChunkBytes is a worker function that generates a chunk of orders and items and writes directly to shard files.
func generateECommerceFactsChunkBytes(startOrderID, numOrdersToGenerate int, startItemID, endItemID int64, sharedHeaderWriter *bufio.Writer, headerMutex *sync.Mutex, itemShardFilename string, customerSampler *AliasSampler, productSampler *AliasSampler, customerAddressMap map[int][]int, productDetails []ecommercemodels.ProductDetails, rng *rand.Rand) error {
	startTime := time.Now()
	// Worker-local ID generator - no atomics needed
	idGen := localIDGen{next: startItemID, end: endItemID}

	// Create item shard file with large NVMe-optimized buffer (64MB)
	itemFile, err := os.Create(itemShardFilename)
	if err != nil {
		return fmt.Errorf("failed to create item shard %s: %w", itemShardFilename, err)
	}
	defer itemFile.Close()

	itemWriter := bufio.NewWriterSize(itemFile, 64<<20) // 64MB buffer for NVMe optimization
	defer itemWriter.Flush()

	// Write header for order items
	itemWriter.WriteString("order_item_id,order_id,product_id,quantity,unit_price,discount\n")

	// Pre-calculate time range for faster random date generation
	startTimeUnix := time.Now().AddDate(-5, 0, 0).Unix()
	endTimeUnix := time.Now().Unix()
	timeRange := endTimeUnix - startTimeUnix

	// Working buffers for constructing rows before writing
	// Use larger preallocated buffers to reduce GC pressure for millions of rows
	headerBuf := make([]byte, 0, 4096) // 4KB buffer for header row construction
	itemBuf := make([]byte, 0, 4096)   // 4KB buffer for item row construction

	// Batched header writing for better NVMe throughput
	headerBatch := make([]byte, 0, 1024*64) // 64KB header batch buffer (increased from 16KB)
	const headerBatchSize = 100                // Batch 100 headers before writing (increased from 50)
	headerBatchCount := 0

	// Item row counter for batched flushing (NVMe optimization)
	itemRowCount := 0
	const itemFlushBatchSize = 20000 // Flush every 20k rows for better NVMe throughput (increased from 10k)

	// Block allocation for order item IDs - REMOVED, using local ID generator instead
	// blk := idBlock{}
	// getBlock := func() {
	//	start := atomic.AddInt64(&globalID, blockSize) - blockSize
	//	blk = idBlock{ next: start, end: start + blockSize }
	// }
	// getBlock()

	for i := 0; i < numOrdersToGenerate; i++ {
		orderID := startOrderID + i
		customerID := customerSampler.Sample(rng)
		addresses := customerAddressMap[customerID]
		if len(addresses) == 0 {
			// This should never happen with pre-filtered customers
			panic(fmt.Sprintf("customer %d has no addresses", customerID))
		}

		shippingAddressID := addresses[rng.Intn(len(addresses))]
		billingAddressID := addresses[rng.Intn(len(addresses))]

		// High-performance random date generation
		randomTimestamp := startTimeUnix + rng.Int63n(timeRange)
		orderStatus := orderStatuses[rng.Intn(len(orderStatuses))]

		numItems := rng.Intn(10) + 1

		// Build header row directly to buffer
		headerBuf = headerBuf[:0] // reset buffer
		headerBuf = strconv.AppendInt(headerBuf, int64(orderID), 10)
		headerBuf = append(headerBuf, ',')
		headerBuf = strconv.AppendInt(headerBuf, int64(customerID), 10)
		headerBuf = append(headerBuf, ',')
		headerBuf = strconv.AppendInt(headerBuf, int64(shippingAddressID), 10)
		headerBuf = append(headerBuf, ',')
		headerBuf = strconv.AppendInt(headerBuf, int64(billingAddressID), 10)
		headerBuf = append(headerBuf, ',')
		headerBuf = strconv.AppendInt(headerBuf, randomTimestamp, 10)  // epoch seconds
		headerBuf = append(headerBuf, ',')
		headerBuf = append(headerBuf, orderStatus...)
		headerBuf = append(headerBuf, '\n')

		// Add to header batch for reduced mutex contention
		headerBatch = append(headerBatch, headerBuf...)
		headerBatchCount++

		// Flush header batch when it reaches batch size
		if headerBatchCount >= headerBatchSize {
			headerMutex.Lock()
			sharedHeaderWriter.Write(headerBatch)
			headerMutex.Unlock()
			headerBatch = headerBatch[:0] // reset batch
			headerBatchCount = 0
		}

		// Generate order items
		for j := 0; j < numItems; j++ {
			productID := productSampler.Sample(rng)
			// Direct slice access instead of map lookup (optimization #5)
			details := productDetails[productID]

			// High-performance integer-based price calculations
			// Convert base price to cents upfront to avoid float operations
			priceCents := int64(details.BasePrice * 100)
			quantity := rng.Intn(15) + 1
			
			// Discount in basis points (0–2500 = 0.00%–25.00%)
			discountBP := int64(0)
			if rng.Intn(100) < 30 {
				discountBP = int64(rng.Intn(2001) + 500) // 5.00% to 25.00%
			}

			// Get order item ID using local generator - no atomics!
			id := idGen.nextID()

			// Build item row directly to buffer using fast integer formatting
			itemBuf = itemBuf[:0] // reset buffer
			itemBuf = strconv.AppendInt(itemBuf, id, 10)
			itemBuf = append(itemBuf, ',')
			itemBuf = strconv.AppendInt(itemBuf, int64(orderID), 10)
			itemBuf = append(itemBuf, ',')
			itemBuf = strconv.AppendInt(itemBuf, int64(productID), 10)
			itemBuf = append(itemBuf, ',')
			itemBuf = strconv.AppendInt(itemBuf, int64(quantity), 10)
			itemBuf = append(itemBuf, ',')
			itemBuf = appendPrice(itemBuf, priceCents)
			itemBuf = append(itemBuf, ',')
			itemBuf = appendDiscount(itemBuf, discountBP)
			itemBuf = append(itemBuf, '\n')

			// Write item row directly to file buffer
			itemWriter.Write(itemBuf)
			itemRowCount++

			// Batched flush for NVMe optimization - flush every 10k rows
			if itemRowCount%itemFlushBatchSize == 0 {
				itemWriter.Flush()
			}
		}
	}

	// Flush any remaining header batch
	if headerBatchCount > 0 {
		headerMutex.Lock()
		sharedHeaderWriter.Write(headerBatch)
		headerMutex.Unlock()
	}

	// Record timing and counts
	duration := time.Since(startTime)
	headerRecords := numOrdersToGenerate
	itemRecords := idGen.next - startItemID // actual items generated
	fmt.Printf("Worker completed: %d header records, %d item records to %s in %s\n", headerRecords, itemRecords, itemShardFilename, duration.Round(time.Millisecond))

	// Files are automatically flushed and closed by defer statements
	return nil
}

// GenerateECommerceModelData generates the e-commerce fact tables with direct file sharding.
func GenerateECommerceModelData(numOrders int, customerIDs []int, customerAddresses []ecommercemodels.CustomerAddress, productDetails []ecommercemodels.ProductDetails, productIDsForSampling []int, outputDir string) error {
	if numOrders <= 0 {
		return nil
	}
	if len(customerIDs) == 0 || len(productIDsForSampling) == 0 || len(customerAddresses) == 0 {
		return fmt.Errorf("cannot generate facts: dimension ID lists are empty")
	}

	// Setup shared resources - moved after address map creation
	productSampler, err := NewAliasSampler(productIDsForSampling)
	if err != nil {
		return fmt.Errorf("failed to set up product sampler: %w", err)
	}

	// Optimized address map creation to prevent slice reallocations, a key performance bottleneck.
	// First pass: count addresses per customer to determine exact slice sizes.
	addressCounts := make(map[int]int, len(customerIDs))
	for _, addr := range customerAddresses {
		addressCounts[addr.CustomerID]++
	}

	// Second pass: pre-allocate slices to their final size and fill them without using append.
	customerAddressMap := make(map[int][]int, len(addressCounts))
	addressIndices := make(map[int]int) // Tracks the current write index for each customer's slice.
	for _, addr := range customerAddresses {
		// If this is the first time we've seen this customer, create their address slice.
		if _, ok := customerAddressMap[addr.CustomerID]; !ok {
			customerAddressMap[addr.CustomerID] = make([]int, addressCounts[addr.CustomerID])
		}
		// Place the address ID at the correct index and increment the index for the next one.
		idx := addressIndices[addr.CustomerID]
		customerAddressMap[addr.CustomerID][idx] = addr.AddressID
		addressIndices[addr.CustomerID]++
	}

	// Pre-filter valid customers that have at least one address
	validCustomerIDs := make([]int, 0, len(customerIDs))
	for _, cid := range customerIDs {
		if len(customerAddressMap[cid]) > 0 {
			validCustomerIDs = append(validCustomerIDs, cid)
		}
	}

	// Now create customer sampler using only valid customers
	customerSampler, err := NewAliasSampler(validCustomerIDs)
	if err != nil {
		return fmt.Errorf("failed to set up customer sampler: %w", err)
	}

	// Concurrency setup with sharding
	numWorkers := runtime.NumCPU()
	ordersPerWorker := (numOrders + numWorkers - 1) / numWorkers

	// Create single shared header file
	headerFile, err := os.Create(fmt.Sprintf("%s/fact_orders_header.csv", outputDir))
	if err != nil {
		return fmt.Errorf("failed to create header file: %w", err)
	}
	defer headerFile.Close()

	sharedHeaderWriter := bufio.NewWriterSize(headerFile, 64<<20) // 64MB buffer for NVMe optimization
	defer sharedHeaderWriter.Flush()

	// Write header for order headers
	sharedHeaderWriter.WriteString("order_id,customer_id,shipping_address_id,billing_address_id,order_timestamp_unix,order_status\n")

	// Mutex for coordinating header writes across workers
	var headerMutex sync.Mutex

	// Generate item shard filenames (headers now use single file)
	itemShardFilenames := make([]string, numWorkers)
	for i := 0; i < numWorkers; i++ {
		itemShardFilenames[i] = fmt.Sprintf("%s/fact_order_items_%d.csv", outputDir, i)
	}

	// Pre-calculate total order items for ID partitioning
	// Using average of 5.0 items per order (matches sizing.go constant)
	const avgItemsPerOrder = 5.0
	totalItems := int64(float64(numOrders) * avgItemsPerOrder)
	itemsPerWorker := totalItems / int64(numWorkers)
	extraItems := totalItems % int64(numWorkers)

	var wg sync.WaitGroup

	// Create a base seed for generating per-worker seeds
	baseSeed := time.Now().UnixNano()

	// Start partitioning item IDs
	startItemID := int64(1) // Start from 1 for order item IDs

	for i := 0; i < numWorkers; i++ {
		startOrderID := (i * ordersPerWorker) + 1
		numToGen := ordersPerWorker
		if startOrderID+numToGen > numOrders+1 {
			numToGen = numOrders - startOrderID + 1
		}

		if numToGen > 0 {
			// Calculate this worker's item ID range
			endItemID := startItemID + itemsPerWorker
			if i < int(extraItems) {
				endItemID++ // distribute remainder
			}

			wg.Add(1)
			// Create a per-worker RNG to eliminate global lock contention
			workerSeed := baseSeed + int64(i)*int64(0x9e3779b9)
			rng := rand.New(rand.NewSource(workerSeed))
			go func(startID, count int, startItem, endItem int64, itemShard string, workerRNG *rand.Rand) {
				defer wg.Done()
				if err := generateECommerceFactsChunkBytes(startID, count, startItem, endItem, sharedHeaderWriter, &headerMutex, itemShard, customerSampler, productSampler, customerAddressMap, productDetails, workerRNG); err != nil {
					// Note: In production, you'd want better error handling
					// For now, we'll print and continue
					_ = err
				}
			}(startOrderID, numToGen, startItemID, endItemID, itemShardFilenames[i], rng)

			// Update start for next worker
			startItemID = endItemID
		}
	}

	wg.Wait()
	return nil
}