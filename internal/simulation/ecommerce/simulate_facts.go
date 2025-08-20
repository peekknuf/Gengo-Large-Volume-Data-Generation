package ecommerce

import (
	"fmt"
	"math"
	"math/rand"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	gf "github.com/brianvoe/gofakeit/v6"

	ecommercemodels "github.com/peekknuf/Gengo/internal/models/ecommerce"
)

var (
	orderStatuses = []string{"Pending", "Processing", "Shipped", "Delivered", "Cancelled", "Returned"}
)

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

func (s *weightedSampler) Sample() int {
	if len(s.ids) == 0 {
		return 0
	}
	if len(s.ids) == 1 {
		return s.ids[0]
	}
	r := rand.Float64()
	// A manual search is more compatible than sort.SearchFloat64s with older Go versions.
	index := 0
	for i, w := range s.cumulativeWeights {
		if r < w {
			index = i
			break
		}
	}
	if index >= len(s.ids) {
		index = len(s.ids) - 1
	}
	return s.ids[index]
}

// generateECommerceFactsChunk is a worker function that generates a chunk of orders and items and sends them to channels.
func generateECommerceFactsChunk(startOrderID, numOrdersToGenerate int, orderItemIDCounter *int64, customerSampler *weightedSampler, productSampler *weightedSampler, customerAddressMap map[int][]int, productInfo map[int]ecommercemodels.ProductDetails, headersChan chan<- ecommercemodels.OrderHeader, itemsChan chan<- ecommercemodels.OrderItem) {
	startTime := time.Now().AddDate(-5, 0, 0)
	endTime := time.Now()

	for i := 0; i < numOrdersToGenerate; i++ {
		orderID := startOrderID + i
		customerID := customerSampler.Sample()
		addresses, ok := customerAddressMap[customerID]
		if !ok || len(addresses) == 0 {
			continue
		}

		shippingAddressID := addresses[rand.Intn(len(addresses))]
		billingAddressID := addresses[rand.Intn(len(addresses))]

		orderTimestamp := gf.DateRange(startTime, endTime)
		orderStatus := orderStatuses[rand.Intn(len(orderStatuses))]

		numItems := rand.Intn(10) + 1

		for j := 0; j < numItems; j++ {
			productID := productSampler.Sample()
			details, ok := productInfo[productID]
			if !ok {
				continue
			}

			quantity := gf.Number(1, 15)
			unitPrice := details.BasePrice
			discount := 0.0
			if rand.Intn(100) < 30 {
				discount = gf.Float64Range(0.05, 0.25)
			}
			totalPrice := float64(quantity)*unitPrice*(1.0-discount)

			newItemID := atomic.AddInt64(orderItemIDCounter, 1)

			orderItem := ecommercemodels.OrderItem{
				OrderItemID: int(newItemID),
				OrderID:     orderID,
				ProductID:   productID,
				Quantity:    quantity,
				UnitPrice:   unitPrice,
				Discount:    discount,
				TotalPrice:  totalPrice,
			}
			itemsChan <- orderItem
		}

		orderHeader := ecommercemodels.OrderHeader{
			OrderID:           orderID,
			CustomerID:        customerID,
			ShippingAddressID: shippingAddressID,
			BillingAddressID:  billingAddressID,
			OrderTimestamp:    orderTimestamp,
			OrderStatus:       orderStatus,
		}
		headersChan <- orderHeader
	}
}

// GenerateECommerceModelData generates the e-commerce fact tables concurrently, streaming data to channels.
func GenerateECommerceModelData(numOrders int, customerIDs []int, customerAddresses []ecommercemodels.CustomerAddress, productInfo map[int]ecommercemodels.ProductDetails, productIDsForSampling []int, headersChan chan<- ecommercemodels.OrderHeader, itemsChan chan<- ecommercemodels.OrderItem) error {
	if numOrders <= 0 {
		return nil
	}
	if len(customerIDs) == 0 || len(productIDsForSampling) == 0 || len(customerAddresses) == 0 {
		return fmt.Errorf("cannot generate facts: dimension ID lists are empty")
	}

	// Close channels when the function exits
	defer close(headersChan)
	defer close(itemsChan)

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
	var orderItemIDCounter int64

	for i := 0; i < numWorkers; i++ {
		startOrderID := (i * ordersPerWorker) + 1
		numToGen := ordersPerWorker
		if startOrderID+numToGen > numOrders+1 {
			numToGen = numOrders - startOrderID + 1
		}

		if numToGen > 0 {
			wg.Add(1)
			go func(startID, count int) {
				defer wg.Done()
				generateECommerceFactsChunk(startID, count, &orderItemIDCounter, customerSampler, productSampler, customerAddressMap, productInfo, headersChan, itemsChan)
			}(startOrderID, numToGen)
		}
	}

	wg.Wait()
	return nil
}