package ecommerce

import (
	"bufio"
	"context"
	"fmt"
	"math"
	"math/rand/v2"
	"strconv"
	"time"

	"github.com/peekknuf/Gengo/internal/formats"
	"github.com/peekknuf/Gengo/internal/generation"
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

// childPrefix gives the exact number of child rows assigned to a prefix of parents.
func childPrefix(parents, children, prefix int) int {
	return prefix*(children/parents) + min(prefix, children%parents)
}

// childCount preserves enough rows for every remaining parent.
func childCount(remaining, parentsAfter, maxPerParent int, draw func(int) int) int {
	low := max(1, remaining-parentsAfter*maxPerParent)
	high := min(maxPerParent, remaining-parentsAfter)
	return low + draw(high-low+1)
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
	if rng.Uint64()>>11 < s.probUint64[i] {
		return s.ids[i]
	}
	return s.ids[s.alias[i]]
}

func GenerateECommerceModelData(numOrders, numItemsTotal int, customerIDs []int, customerAddresses []ecommercemodels.CustomerAddress, productDetails []ecommercemodels.ProductDetails, productIDs []int, outputDir, format string, cfg generation.Config) error {
	if numOrders < 0 || numItemsTotal < numOrders || int64(numItemsTotal) > 10*int64(numOrders) || numItemsTotal > math.MaxInt32 {
		return fmt.Errorf("order items require 1 to 10 rows per order and IDs within int32")
	}
	if numOrders == 0 {
		return nil
	}
	products, err := NewAliasSampler(productIDs)
	if err != nil {
		return err
	}
	customers, err := NewAliasSampler(customerIDs)
	if err != nil {
		return err
	}
	addresses := make(map[int][]int, len(customerIDs))
	for _, a := range customerAddresses {
		addresses[a.CustomerID] = append(addresses[a.CustomerID], a.AddressID)
	}
	for _, id := range customerIDs {
		if len(addresses[id]) == 0 {
			return fmt.Errorf("customer %d has no address", id)
		}
	}
	startTime := cfg.AsOf.AddDate(-5, 0, 0).Unix()
	span := cfg.AsOf.AddDate(0, 0, 1).Unix() - startTime
	return generation.Partitions(context.Background(), numOrders, cfg, func(ctx context.Context, p generation.Partition) error {
		rng := cfg.RandV2("ecommerce.orders", uint64(p.Start))
		firstItem := childPrefix(numOrders, numItemsTotal, p.Start) + 1
		endItem := childPrefix(numOrders, numItemsTotal, p.Start+p.Count) + 1
		nextItem := firstItem
		headers := make([]ecommercemodels.OrderHeader, 0, p.Count)
		items := make([]ecommercemodels.OrderItem, 0, endItem-firstItem)
		for i := 0; i < p.Count; i++ {
			if i%1024 == 0 && ctx.Err() != nil {
				return ctx.Err()
			}
			orderID := p.Start + i + 1
			customer := customers.Sample(rng)
			customerAddresses := addresses[customer]
			headers = append(headers, ecommercemodels.OrderHeader{
				OrderID: orderID, CustomerID: customer,
				ShippingAddressID: customerAddresses[rng.IntN(len(customerAddresses))],
				BillingAddressID:  customerAddresses[rng.IntN(len(customerAddresses))],
				OrderTimestamp:    time.Unix(startTime+rng.Int64N(span), 0).UTC(),
				OrderStatus:       orderStatuses[rng.IntN(len(orderStatuses))],
			})
			count := childCount(endItem-nextItem, p.Count-i-1, 10, rng.IntN)
			for j := 0; j < count; j++ {
				product := products.Sample(rng)
				quantity := rng.IntN(15) + 1
				discount := 0.0
				if rng.IntN(100) < 30 {
					discount = float64(rng.IntN(2001)+500) / 10000
				}
				items = append(items, ecommercemodels.OrderItem{OrderItemID: nextItem, OrderID: orderID, ProductID: product, Quantity: quantity, UnitPrice: productDetails[product].BasePrice, Discount: discount})
				nextItem++
			}
		}
		headerPath := fmt.Sprintf("%s/fact_orders_header_%d.%s", outputDir, p.Index, format)
		itemPath := fmt.Sprintf("%s/fact_order_items_%d.%s", outputDir, p.Index, format)
		if format == "parquet" {
			if err := formats.WriteOrderHeadersToParquetTyped(headers, headerPath); err != nil {
				return err
			}
			if err := formats.WriteOrderItemsToParquetTyped(items, itemPath); err != nil {
				return err
			}
		} else if err := writeOrderCSV(headers, items, headerPath, itemPath); err != nil {
			return err
		}
		if err := cfg.RecordFile(headerPath, len(headers), int64(p.Start)+1, int64(p.Start+p.Count)); err != nil {
			return err
		}
		return cfg.RecordFile(itemPath, len(items), int64(firstItem), int64(nextItem-1))
	})
}

func writeOrderCSV(headers []ecommercemodels.OrderHeader, items []ecommercemodels.OrderItem, headerPath, itemPath string) error {
	err := formats.WriteCSVFile(headerPath, func(w *bufio.Writer) error {
		if _, err := w.WriteString("order_id,customer_id,shipping_address_id,billing_address_id,order_timestamp_unix,order_status\n"); err != nil {
			return err
		}
		buf := make([]byte, 0, 256)
		for _, h := range headers {
			buf = fastItoa(buf[:0], int64(h.OrderID))
			buf = append(buf, ',')
			buf = fastItoa(buf, int64(h.CustomerID))
			buf = append(buf, ',')
			buf = fastItoa(buf, int64(h.ShippingAddressID))
			buf = append(buf, ',')
			buf = fastItoa(buf, int64(h.BillingAddressID))
			buf = append(buf, ',')
			buf = strconv.AppendInt(buf, h.OrderTimestamp.Unix(), 10)
			buf = append(buf, ',')
			buf = append(buf, h.OrderStatus...)
			buf = append(buf, '\n')
			if _, err := w.Write(buf); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	return formats.WriteCSVFile(itemPath, func(w *bufio.Writer) error {
		if _, err := w.WriteString("order_item_id,order_id,product_id,quantity,unit_price,discount\n"); err != nil {
			return err
		}
		buf := make([]byte, 0, 128)
		for _, item := range items {
			buf = fastItoa(buf[:0], int64(item.OrderItemID))
			buf = append(buf, ',')
			buf = fastItoa(buf, int64(item.OrderID))
			buf = append(buf, ',')
			buf = fastItoa(buf, int64(item.ProductID))
			buf = append(buf, ',')
			buf = fastItoa(buf, int64(item.Quantity))
			buf = append(buf, ',')
			buf = strconv.AppendFloat(buf, item.UnitPrice, 'f', -1, 64)
			buf = append(buf, ',')
			buf = strconv.AppendFloat(buf, item.Discount, 'f', -1, 64)
			buf = append(buf, '\n')
			if _, err := w.Write(buf); err != nil {
				return err
			}
		}
		return nil
	})
}
