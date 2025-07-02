package ecommerce

import (
	"fmt"
	"math"
	"math/rand"
	
	"sort"
	"time"

	gf "github.com/brianvoe/gofakeit/v6"

	"github.com/peekknuf/Gengo/internal/models/ecommerce"
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
	index := sort.SearchFloat64s(s.cumulativeWeights, r)
	if index >= len(s.ids) {
		index = len(s.ids) - 1
	}
	return s.ids[index]
}

func GenerateECommerceModelData(numOrders int, customerIDs []int, customerAddresses []ecommerce.CustomerAddress, productInfo map[int]ecommerce.ProductDetails, productIDsForSampling []int) ([]ecommerce.OrderHeader, []ecommerce.OrderItem, error) {
	if numOrders <= 0 {
		return nil, nil, nil
	}
	if len(customerIDs) == 0 || len(productIDsForSampling) == 0 || len(customerAddresses) == 0 {
		return nil, nil, fmt.Errorf("cannot generate facts: dimension ID lists are empty")
	}

	customerSampler, err := setupWeightedSampler(customerIDs)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to set up customer sampler: %w", err)
	}
	productSampler, err := setupWeightedSampler(productIDsForSampling)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to set up product sampler: %w", err)
	}

	customerAddressMap := make(map[int][]int)
	for _, addr := range customerAddresses {
		customerAddressMap[addr.CustomerID] = append(customerAddressMap[addr.CustomerID], addr.AddressID)
	}

	headers := make([]ecommerce.OrderHeader, numOrders)
	items := make([]ecommerce.OrderItem, 0, numOrders*3)
	orderItemIDCounter := 1

	gf.Seed(time.Now().UnixNano())
	startTime := time.Now().AddDate(-5, 0, 0)
	endTime := time.Now()

	for i := 0; i < numOrders; i++ {
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
		var totalOrderAmount float64

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
			totalOrderAmount += totalPrice

			orderItem := ecommerce.OrderItem{
				OrderItemID: orderItemIDCounter,
				OrderID:     i + 1,
				ProductID:   productID,
				Quantity:    quantity,
				UnitPrice:   unitPrice,
				Discount:    discount,
				TotalPrice:  totalPrice,
			}
			items = append(items, orderItem)
			orderItemIDCounter++
		}

		headers[i] = ecommerce.OrderHeader{
			OrderID:           i + 1,
			CustomerID:        customerID,
			ShippingAddressID: shippingAddressID,
			BillingAddressID:  billingAddressID,
			OrderTimestamp:    orderTimestamp,
			OrderStatus:       orderStatus,
			TotalOrderAmount:  totalOrderAmount,
		}

		
	}
	return headers, items, nil
}
