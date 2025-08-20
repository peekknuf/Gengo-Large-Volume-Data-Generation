package ecommerce

import (
	"fmt"
	"math"
	"math/rand"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	ecommercemodels "github.com/peekknuf/Gengo/internal/models/ecommerce"
)

var (
	orderStatuses = []string{"Pending", "Processing", "Shipped", "Delivered", "Cancelled", "Returned"}
)

const blockSize = 100_000

type idBlock struct{ next, end int64 }

func (b *idBlock) nextID() int64 {
    if b.next == b.end { return -1 }
    v := b.next; b.next++
    return v
}

// Global ID counter for order items
var globalID int64

type weightedSampler struct {
	ids               []int
	cumulativeWeights []float64
}

func setupWeightedSampler(ids []int) (*weightedSampler, error) {
	n := len(ids)
	if n == 0 {
		return nil, fmt.Errorf("cannot create sampler from empty ID list")
	}

	sampler := &weightedSampler{
		ids:               make([]int, n),
		cumulativeWeights: make([]float64, n),
	}
	copy(sampler.ids, ids)

	var totalRawWeight float64
	for i := 0; i < n; i++ {
		rawWeight := 1.0 / math.Sqrt(float64(i+1))
		totalRawWeight += rawWeight
		sampler.cumulativeWeights[i] = totalRawWeight
	}

	if totalRawWeight > 0 {
		for i := 0; i < n; i++ {
			sampler.cumulativeWeights[i] /= totalRawWeight
		}
		sampler.cumulativeWeights[n-1] = 1.0
	} else {
		return nil, fmt.Errorf("total raw weight is zero")
	}

	return sampler, nil
}

func (s *weightedSampler) Sample(rng *rand.Rand) int {
	if len(s.ids) == 0 {
		return 0
	}
	if len(s.ids) == 1 {
		return s.ids[0]
	}
	r := rng.Float64()
	// Binary search for better performance with large ID sets
	i, j := 0, len(s.cumulativeWeights)
	for i < j {
		h := int(uint(i+j) >> 1) // avoid overflow when computing h
		if s.cumulativeWeights[h] < r {
			i = h + 1
		} else {
			j = h
		}
	}
	if i >= len(s.ids) {
		i = len(s.ids) - 1
	}
	return s.ids[i]
}

// generateECommerceFactsChunkBytes is a worker function that generates a chunk of orders and items and sends byte chunks to channels.
func generateECommerceFactsChunkBytes(startOrderID, numOrdersToGenerate int, customerSampler *weightedSampler, productSampler *weightedSampler, customerAddressMap map[int][]int, productDetails []ecommercemodels.ProductDetails, headersChunkChan chan<- []byte, itemsChunkChan chan<- []byte, rng *rand.Rand) {
	// Pre-calculate time range for faster random date generation
	startTimeUnix := time.Now().AddDate(-5, 0, 0).Unix()
	endTimeUnix := time.Now().Unix()
	timeRange := endTimeUnix - startTimeUnix

	// Buffers for building byte chunks (increased to 16MB as requested)
	headersBuf := make([]byte, 0, 16<<20) // 16MB
	itemsBuf := make([]byte, 0, 16<<20)   // 16MB

	// Helper function to flush buffers
	flush := func(ch chan<- []byte, buf *[]byte) {
		if len(*buf) == 0 {
			return
		}
		out := make([]byte, len(*buf))
		copy(out, *buf)
		ch <- out
		*buf = (*buf)[:0]
	}

	// Block allocation for order item IDs
	blk := idBlock{}
	getBlock := func() {
		start := atomic.AddInt64(&globalID, blockSize) - blockSize
		blk = idBlock{ next: start, end: start + blockSize }
	}
	getBlock()

	for i := 0; i < numOrdersToGenerate; i++ {
		orderID := startOrderID + i
		customerID := customerSampler.Sample(rng)
		addresses, ok := customerAddressMap[customerID]
		if !ok || len(addresses) == 0 {
			continue
		}

		shippingAddressID := addresses[rng.Intn(len(addresses))]
		billingAddressID := addresses[rng.Intn(len(addresses))]

		// High-performance random date generation
		randomTimestamp := startTimeUnix + rng.Int63n(timeRange)
		orderStatus := orderStatuses[rng.Intn(len(orderStatuses))]

		numItems := rng.Intn(10) + 1

		// Build header row as byte chunk (using epoch seconds instead of formatted timestamp)
		headersBuf = strconv.AppendInt(headersBuf, int64(orderID), 10)
		headersBuf = append(headersBuf, ',')
		headersBuf = strconv.AppendInt(headersBuf, int64(customerID), 10)
		headersBuf = append(headersBuf, ',')
		headersBuf = strconv.AppendInt(headersBuf, int64(shippingAddressID), 10)
		headersBuf = append(headersBuf, ',')
		headersBuf = strconv.AppendInt(headersBuf, int64(billingAddressID), 10)
		headersBuf = append(headersBuf, ',')
		headersBuf = strconv.AppendInt(headersBuf, randomTimestamp, 10)  // epoch seconds
		headersBuf = append(headersBuf, ',')
		headersBuf = append(headersBuf, orderStatus...)
		headersBuf = append(headersBuf, '\n')

		// Check if header buffer needs to be flushed
		if len(headersBuf) >= 16<<20 {
			flush(headersChunkChan, &headersBuf)
		}

		// Generate order items
		for j := 0; j < numItems; j++ {
			productID := productSampler.Sample(rng)
			// Direct slice access instead of map lookup (optimization #5)
			details := productDetails[productID]

			// High-performance random number generation
			quantity := rng.Intn(15) + 1
			unitPrice := details.BasePrice
			discount := 0.0
			if rng.Intn(100) < 30 {
				discount = 0.05 + rng.Float64()*0.20
			}
			totalPrice := float64(quantity)*unitPrice*(1.0-discount)

			// Get order item ID using block allocation
			id := blk.nextID()
			if id < 0 {
				getBlock()
				id = blk.nextID()
			}

			// Build item row as byte chunk
			itemsBuf = strconv.AppendInt(itemsBuf, id, 10)
			itemsBuf = append(itemsBuf, ',')
			itemsBuf = strconv.AppendInt(itemsBuf, int64(orderID), 10)
			itemsBuf = append(itemsBuf, ',')
			itemsBuf = strconv.AppendInt(itemsBuf, int64(productID), 10)
			itemsBuf = append(itemsBuf, ',')
			itemsBuf = strconv.AppendInt(itemsBuf, int64(quantity), 10)
			itemsBuf = append(itemsBuf, ',')
			itemsBuf = strconv.AppendFloat(itemsBuf, unitPrice, 'f', 2, 64)
			itemsBuf = append(itemsBuf, ',')
			itemsBuf = strconv.AppendFloat(itemsBuf, discount, 'f', 4, 64)
			itemsBuf = append(itemsBuf, ',')
			itemsBuf = strconv.AppendFloat(itemsBuf, totalPrice, 'f', 4, 64)
			itemsBuf = append(itemsBuf, '\n')

			// Check if item buffer needs to be flushed
			if len(itemsBuf) >= 16<<20 {
				flush(itemsChunkChan, &itemsBuf)
			}
		}
	}

	// Flush any remaining data in buffers
	flush(headersChunkChan, &headersBuf)
	flush(itemsChunkChan, &itemsBuf)
}

// GenerateECommerceModelData generates the e-commerce fact tables concurrently, streaming data as byte chunks.
func GenerateECommerceModelData(numOrders int, customerIDs []int, customerAddresses []ecommercemodels.CustomerAddress, productDetails []ecommercemodels.ProductDetails, productIDsForSampling []int, headersChunkChan chan<- []byte, itemsChunkChan chan<- []byte) error {
	if numOrders <= 0 {
		close(headersChunkChan)
		close(itemsChunkChan)
		return nil
	}
	if len(customerIDs) == 0 || len(productIDsForSampling) == 0 || len(customerAddresses) == 0 {
		close(headersChunkChan)
		close(itemsChunkChan)
		return fmt.Errorf("cannot generate facts: dimension ID lists are empty")
	}

	// This function is the parent of the worker goroutines. It's responsible
	// for closing the channels when all workers are done.
	defer close(headersChunkChan)
	defer close(itemsChunkChan)

	// Setup shared resources
	customerSampler, err := setupWeightedSampler(customerIDs)
	if err != nil {
		return fmt.Errorf("failed to set up customer sampler: %w", err)
	}
	productSampler, err := setupWeightedSampler(productIDsForSampling)
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

	// Concurrency setup
	numWorkers := runtime.NumCPU()
	ordersPerWorker := (numOrders + numWorkers - 1) / numWorkers
	var wg sync.WaitGroup

	// Create a base seed for generating per-worker seeds
	baseSeed := time.Now().UnixNano()

	for i := 0; i < numWorkers; i++ {
		startOrderID := (i * ordersPerWorker) + 1
		numToGen := ordersPerWorker
		if startOrderID+numToGen > numOrders+1 {
			numToGen = numOrders - startOrderID + 1
		}

		if numToGen > 0 {
			wg.Add(1)
			// Create a per-worker RNG to eliminate global lock contention
			workerSeed := baseSeed + int64(i)*int64(0x9e3779b9)
			rng := rand.New(rand.NewSource(workerSeed))
			go func(startID, count int, workerRNG *rand.Rand) {
				defer wg.Done()
				generateECommerceFactsChunkBytes(startID, count, customerSampler, productSampler, customerAddressMap, productDetails, headersChunkChan, itemsChunkChan, workerRNG)
			}(startOrderID, numToGen, rng)
		}
	}

	wg.Wait()
	return nil
}