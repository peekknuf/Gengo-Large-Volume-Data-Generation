package ecommerce

import (
	"bufio"
	"fmt"
	"math"
	"math/rand/v2"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/peekknuf/Gengo/internal/formats"
	ecommercemodels "github.com/peekknuf/Gengo/internal/models/ecommerce"
)

var (
	orderStatuses = []string{"Pending", "Processing", "Shipped", "Delivered", "Cancelled", "Returned"}
	digitsLUT     = [100]string{
		"00", "01", "02", "03", "04", "05", "06", "07", "08", "09",
		"10", "11", "12", "13", "14", "15", "16", "17", "18", "19",
		"20", "21", "22", "23", "24", "25", "26", "27", "28", "29",
		"30", "31", "32", "33", "34", "35", "36", "37", "38", "39",
		"40", "41", "42", "43", "44", "45", "46", "47", "48", "49",
		"50", "51", "52", "53", "54", "55", "56", "57", "58", "59",
		"60", "61", "62", "63", "64", "65", "66", "67", "68", "69",
		"70", "71", "72", "73", "74", "75", "76", "77", "78", "79",
		"80", "81", "82", "83", "84", "85", "86", "87", "88", "89",
		"90", "91", "92", "93", "94", "95", "96", "97", "98", "99",
	}
)

func fastItoa(buf []byte, v int64) []byte {
	if v < 10 {
		return append(buf, byte('0'+v))
	}
	var tmp [20]byte
	pos := len(tmp)
	for v >= 100 {
		pos -= 2
		copy(tmp[pos:], digitsLUT[v%100])
		v /= 100
	}
	if v < 10 {
		pos--
		tmp[pos] = byte('0' + v)
	} else {
		pos -= 2
		copy(tmp[pos:], digitsLUT[v])
	}
	return append(buf, tmp[pos:]...)
}

func appendPrice(buf []byte, cents int64) []byte {
	if cents < 0 {
		buf = append(buf, '-')
		cents = -cents
	}
	buf = fastItoa(buf, cents/100)
	buf = append(buf, '.')
	frac := cents % 100
	if frac < 10 {
		buf = append(buf, '0')
	}
	buf = fastItoa(buf, frac)
	return buf
}

func appendDiscount(buf []byte, basisPoints int64) []byte {
	if basisPoints < 0 {
		buf = append(buf, '-')
		basisPoints = -basisPoints
	}
	buf = fastItoa(buf, basisPoints/10000)
	buf = append(buf, '.')
	frac := basisPoints % 10000
	if frac < 1000 {
		buf = append(buf, '0')
	}
	if frac < 100 {
		buf = append(buf, '0')
	}
	if frac < 10 {
		buf = append(buf, '0')
	}
	buf = fastItoa(buf, frac)
	return buf
}

type idBlock struct{ next, end int64 }

func (b *idBlock) nextID() int64 {
	if b.next == b.end {
		return -1
	}
	v := b.next
	b.next++
	return v
}

type AliasSampler struct {
	probUint64 []uint64
	alias      []int
	ids        []int
	n          int
}

func NewAliasSampler(ids []int) (*AliasSampler, error) {
	n := len(ids)
	if n == 0 {
		return nil, fmt.Errorf("cannot build sampler from empty ID list")
	}
	if n == 1 {
		return &AliasSampler{ids: ids, probUint64: []uint64{1 << 53}, alias: []int{0}, n: 1}, nil
	}

	weights := make([]float64, n)
	var total float64
	for i := 0; i < n; i++ {
		w := 1.0 / math.Sqrt(float64(i+1))
		weights[i] = w
		total += w
	}

	for i := 0; i < n; i++ {
		weights[i] = weights[i] * float64(n) / total
	}

	prob := make([]float64, n)
	alias := make([]int, n)

	small := make([]int, 0, n)
	large := make([]int, 0, n)

	for i, w := range weights {
		if w < 1.0 {
			small = append(small, i)
		} else {
			large = append(large, i)
		}
	}

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

	for _, i := range append(small, large...) {
		prob[i] = 1.0
		alias[i] = i
	}

	probUint64 := make([]uint64, n)
	for i := 0; i < n; i++ {
		probUint64[i] = uint64(prob[i] * (1 << 53))
	}

	return &AliasSampler{
		probUint64: probUint64,
		alias:      alias,
		ids:        append([]int(nil), ids...),
		n:          n,
	}, nil
}

func (s *AliasSampler) Sample(rng *rand.Rand) int {
	if s.n == 1 {
		return s.ids[0]
	}
	raw := rng.Uint64()
	i := int(raw % uint64(s.n))
	if rng.Uint64() < s.probUint64[i] {
		return s.ids[i]
	}
	return s.ids[s.alias[i]]
}

func GenerateECommerceModelData(numOrders int, customerIDs []int, customerAddresses []ecommercemodels.CustomerAddress, productDetails []ecommercemodels.ProductDetails, productIDsForSampling []int, outputDir string, format string) error {
	if numOrders <= 0 {
		return nil
	}
	if len(customerIDs) == 0 || len(productIDsForSampling) == 0 || len(customerAddresses) == 0 {
		return fmt.Errorf("cannot generate facts: dimension ID lists are empty")
	}

	maxCustomerID := 0
	for _, cid := range customerIDs {
		if cid > maxCustomerID {
			maxCustomerID = cid
		}
	}
	customerAddressSlice := make([][]int, maxCustomerID+1)
	for _, addr := range customerAddresses {
		customerAddressSlice[addr.CustomerID] = append(customerAddressSlice[addr.CustomerID], addr.AddressID)
	}

	if format == "csv" {
		return generateECommerceModelDataCSV(numOrders, customerIDs, customerAddressSlice, productDetails, productIDsForSampling, outputDir)
	}
	return generateECommerceModelDataParquet(numOrders, customerIDs, customerAddressSlice, productDetails, productIDsForSampling, outputDir)
}

func generateECommerceModelDataCSV(numOrders int, customerIDs []int, customerAddressSlice [][]int, productDetails []ecommercemodels.ProductDetails, productIDsForSampling []int, outputDir string) error {
	productSampler, err := NewAliasSampler(productIDsForSampling)
	if err != nil {
		return fmt.Errorf("failed to set up product sampler: %w", err)
	}

	customerSampler, err := NewAliasSampler(customerIDs)
	if err != nil {
		return fmt.Errorf("failed to set up customer sampler: %w", err)
	}

	numWorkers := runtime.NumCPU()
	ordersPerWorker := (numOrders + numWorkers - 1) / numWorkers

	headerFile, err := os.Create(fmt.Sprintf("%s/fact_orders_header.csv", outputDir))
	if err != nil {
		return fmt.Errorf("failed to create header file: %w", err)
	}
	defer headerFile.Close()

	sharedHeaderWriter := bufio.NewWriterSize(headerFile, 64<<20)
	defer sharedHeaderWriter.Flush()

	sharedHeaderWriter.WriteString("order_id,customer_id,shipping_address_id,billing_address_id,order_timestamp_unix,order_status\n")

	var headerMutex sync.Mutex

	itemShardFilenames := make([]string, numWorkers)
	for i := 0; i < numWorkers; i++ {
		itemShardFilenames[i] = fmt.Sprintf("%s/fact_order_items_%d.csv", outputDir, i)
	}

	const avgItemsPerOrder = 11.0
	totalItems := int64(float64(numOrders) * avgItemsPerOrder)
	itemsPerWorker := totalItems / int64(numWorkers)
	extraItems := totalItems % int64(numWorkers)

	var wg sync.WaitGroup
	baseSeed := time.Now().UnixNano()
	startItemID := int64(1)

	for i := 0; i < numWorkers; i++ {
		startOrderID := (i * ordersPerWorker) + 1
		numToGen := ordersPerWorker
		if startOrderID+numToGen > numOrders+1 {
			numToGen = numOrders - startOrderID + 1
		}

		if numToGen > 0 {
			endItemID := startItemID + itemsPerWorker
			if i < int(extraItems) {
				endItemID++
			}

			wg.Add(1)
			go func(workerID, startOrderID, numToGen int, startItemID, endItemID int64) {
				defer wg.Done()

				rng := rand.New(rand.NewPCG(uint64(baseSeed), uint64(workerID)))
				idGen := &idBlock{next: startItemID, end: endItemID}

				itemFile, err := os.Create(itemShardFilenames[workerID])
				if err != nil {
					return
				}
				defer itemFile.Close()

				itemWriter := bufio.NewWriterSize(itemFile, 64<<20)
				defer itemWriter.Flush()

				itemWriter.WriteString("order_item_id,order_id,product_id,quantity,unit_price,discount\n")

				headerBatchSize := 1000
				headerBatch := make([]byte, 0, 256*1024)
				headerBatchCount := 0
				var itemBuf []byte
				orderIDBuf := make([]byte, 0, 20)

				for orderID := startOrderID; orderID < startOrderID+numToGen; orderID++ {
					customerID := customerSampler.Sample(rng)
					addresses := customerAddressSlice[customerID]
					shippingAddressID := addresses[int(rng.Uint64()%uint64(len(addresses)))]
					billingAddressID := addresses[int(rng.Uint64()%uint64(len(addresses)))]

					orderTimestamp := int64(rng.Uint64()%(5*31536000)) + 1577836800
					orderStatus := orderStatuses[int(rng.Uint64()%uint64(len(orderStatuses)))]

					orderIDBuf = orderIDBuf[:0]
					orderIDBuf = fastItoa(orderIDBuf, int64(orderID))

					headerBatch = append(headerBatch, orderIDBuf...)
					headerBatch = append(headerBatch, ',')
					headerBatch = fastItoa(headerBatch, int64(customerID))
					headerBatch = append(headerBatch, ',')
					headerBatch = fastItoa(headerBatch, int64(shippingAddressID))
					headerBatch = append(headerBatch, ',')
					headerBatch = fastItoa(headerBatch, int64(billingAddressID))
					headerBatch = append(headerBatch, ',')
					headerBatch = fastItoa(headerBatch, orderTimestamp)
					headerBatch = append(headerBatch, ',')
					headerBatch = append(headerBatch, orderStatus...)
					headerBatch = append(headerBatch, '\n')
					headerBatchCount++

					if headerBatchCount >= headerBatchSize {
						headerMutex.Lock()
						sharedHeaderWriter.Write(headerBatch)
						headerMutex.Unlock()
						headerBatch = headerBatch[:0]
						headerBatchCount = 0
					}

					numItems := int(rng.Uint64()%10) + 1
					for j := 0; j < numItems; j++ {
						productID := productSampler.Sample(rng)
						details := productDetails[productID]

						priceCents := int64(details.BasePrice * 100)
						quantity := int(rng.Uint64()%15) + 1

						discountBP := int64(0)
						if rng.Uint64()%100 < 30 {
							discountBP = int64(rng.Uint64()%2001) + 500
						}

						id := idGen.nextID()

						itemBuf = itemBuf[:0]
						itemBuf = fastItoa(itemBuf, id)
						itemBuf = append(itemBuf, ',')
						itemBuf = append(itemBuf, orderIDBuf...)
						itemBuf = append(itemBuf, ',')
						itemBuf = fastItoa(itemBuf, int64(productID))
						itemBuf = append(itemBuf, ',')
						itemBuf = fastItoa(itemBuf, int64(quantity))
						itemBuf = append(itemBuf, ',')
						itemBuf = appendPrice(itemBuf, priceCents)
						itemBuf = append(itemBuf, ',')
						itemBuf = appendDiscount(itemBuf, discountBP)
						itemBuf = append(itemBuf, '\n')

						itemWriter.Write(itemBuf)
					}
				}

				if headerBatchCount > 0 {
					headerMutex.Lock()
					sharedHeaderWriter.Write(headerBatch)
					headerMutex.Unlock()
				}
			}(i, startOrderID, numToGen, startItemID, endItemID)

			startItemID = endItemID
		}
	}

	wg.Wait()
	return nil
}

func generateECommerceModelDataParquet(numOrders int, customerIDs []int, customerAddressSlice [][]int, productDetails []ecommercemodels.ProductDetails, productIDsForSampling []int, outputDir string) error {
	productSampler, err := NewAliasSampler(productIDsForSampling)
	if err != nil {
		return fmt.Errorf("failed to set up product sampler: %w", err)
	}

	customerSampler, err := NewAliasSampler(customerIDs)
	if err != nil {
		return fmt.Errorf("failed to set up customer sampler: %w", err)
	}

	numWorkers := runtime.NumCPU()
	ordersPerWorker := (numOrders + numWorkers - 1) / numWorkers

	const avgItemsPerOrder = 11.0
	totalItems := int64(float64(numOrders) * avgItemsPerOrder)
	itemsPerWorker := totalItems / int64(numWorkers)
	extraItems := totalItems % int64(numWorkers)

	var wg sync.WaitGroup
	errChan := make(chan error, numWorkers*2)

	startItemID := int64(1)

	for i := 0; i < numWorkers; i++ {
		startOrderID := (i * ordersPerWorker) + 1
		numToGen := ordersPerWorker
		if startOrderID+numToGen > numOrders+1 {
			numToGen = numOrders - startOrderID + 1
		}

		if numToGen <= 0 {
			continue
		}

		endItemID := startItemID + itemsPerWorker
		if i < int(extraItems) {
			endItemID++
		}

		wg.Add(1)
		go func(workerID, startOrderID, numToGen int, startItemID, endItemID int64) {
			defer wg.Done()

			rng := rand.New(rand.NewPCG(uint64(time.Now().UnixNano()), uint64(workerID)))
			idGen := &idBlock{next: startItemID, end: endItemID}

			headers := make([]ecommercemodels.OrderHeader, 0, numToGen)
			items := make([]ecommercemodels.OrderItem, 0, int(float64(numToGen)*avgItemsPerOrder))

			for orderID := startOrderID; orderID < startOrderID+numToGen; orderID++ {
				customerID := customerSampler.Sample(rng)
				addresses := customerAddressSlice[customerID]
				shippingAddressID := addresses[rng.IntN(len(addresses))]
				billingAddressID := addresses[rng.IntN(len(addresses))]

				orderTimestamp := time.Unix(rng.Int64N(31536000)+31536000, 0)
				orderStatus := orderStatuses[rng.IntN(len(orderStatuses))]

				headers = append(headers, ecommercemodels.OrderHeader{
					OrderID:           orderID,
					CustomerID:        customerID,
					ShippingAddressID: shippingAddressID,
					BillingAddressID:  billingAddressID,
					OrderTimestamp:    orderTimestamp,
					OrderStatus:       orderStatus,
				})

				numItems := rng.IntN(10) + 1
				for j := 0; j < numItems; j++ {
					productID := productSampler.Sample(rng)
					details := productDetails[productID]

					quantity := rng.IntN(15) + 1
					unitPrice := details.BasePrice

					discount := 0.0
					if rng.IntN(100) < 30 {
						discount = float64(rng.IntN(2001)+500) / 10000.0
					}

					itemID := idGen.nextID()
					if itemID < 0 {
						itemID = startItemID + int64(cap(items))
					}

					items = append(items, ecommercemodels.OrderItem{
						OrderItemID: int(itemID),
						OrderID:     orderID,
						ProductID:   productID,
						Quantity:    quantity,
						UnitPrice:   unitPrice,
						Discount:    discount,
					})
				}
			}

			headerFilename := fmt.Sprintf("%s/fact_orders_header_%d.parquet", outputDir, workerID)
			if err := formats.WriteOrderHeadersToParquetTyped(headers, headerFilename); err != nil {
				errChan <- fmt.Errorf("failed to write order headers for worker %d: %w", workerID, err)
				return
			}

			itemFilename := fmt.Sprintf("%s/fact_order_items_%d.parquet", outputDir, workerID)
			if err := formats.WriteOrderItemsToParquetTyped(items, itemFilename); err != nil {
				errChan <- fmt.Errorf("failed to write order items for worker %d: %w", workerID, err)
				return
			}
		}(i, startOrderID, numToGen, startItemID, endItemID)

		startItemID = endItemID
	}

	wg.Wait()
	close(errChan)

	for err := range errChan {
		if err != nil {
			return err
		}
	}

	return nil
}
