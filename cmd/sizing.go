// cmd/sizing.go
package cmd

import (
	"fmt"
	"math"
)

// --- Configuration Constants ---

// Default Ratios (These are assumptions, adjust based on desired data shape)
const (
	// How many orders, on average, are associated with a single customer?
	DefaultOrdersPerCustomerRatio float64 = 10.0
	// How many orders, on average, are placed for each unique product available?
	DefaultOrdersPerProductRatio float64 = 50.0
	// How many orders, on average, reference a unique location (shipping or billing)?
	// Location reuse is high, so fewer unique locations needed per order.
	DefaultOrdersPerLocationRatio float64 = 100.0
)

// Estimated Average Size in Bytes for Different Field Types (Uncompressed)
// These are rough estimates and can be tuned. Strings are the most variable.
const (
	AvgBytesStringTiny    = 8   // e.g., State abbreviation, maybe status
	AvgBytesStringShort   = 15  // e.g., First name, Last name, City
	AvgBytesStringMedium  = 30  // e.g., Email, Product Name, Category, Address Line 1
	AvgBytesStringLong    = 50  // e.g., Longer addresses, Company names
	SizeBytesInt          = 4   // Assuming int32 for IDs and Quantity
	SizeBytesFloat64      = 8   // Price, Discount
	SizeBytesTimestamp    = 8   // UnixMicro or similar binary representation; if string (RFC3339), use AvgBytesStringMedium
	SizeBytesAssumedTotal = 250 // A fallback rough estimate for a whole OrderFact row if needed initially
)

// RowCounts holds the calculated number of rows for each table
type RowCounts struct {
	Customers int
	Products  int
	Locations int
	Orders    int
}

// estimateAvgRowSizeBytes estimates the average uncompressed size of a single row for a given table type.
func estimateAvgRowSizeBytes(tableType string) (int, error) {
	switch tableType {
	case "customer":
		// CustomerID(int) + FirstName(short) + LastName(short) + Email(medium)
		return SizeBytesInt + AvgBytesStringShort + AvgBytesStringShort + AvgBytesStringMedium, nil
	case "product":
		// ProductID(int) + ProductName(medium) + ProductCategory(medium) + Company(long) + BasePrice(float64)
		return SizeBytesInt + AvgBytesStringMedium + AvgBytesStringMedium + AvgBytesStringLong + SizeBytesFloat64, nil
	case "location":
		// LocationID(int) + Address(medium) + City(short) + State(tiny) + Zip(tiny) + Country(short)
		return SizeBytesInt + AvgBytesStringMedium + AvgBytesStringShort + AvgBytesStringTiny + AvgBytesStringTiny + AvgBytesStringShort, nil
	case "orderfact":
		// OrderID(int) + Timestamp(ts) + CustomerID(int) + ProductID(int) + ShipLocID(int) + BillLocID(int) +
		// Quantity(int) + UnitPrice(float64) + Discount(float64) + TotalPrice(float64) + Status(tiny) + Payment(short)
		return SizeBytesInt + SizeBytesTimestamp + SizeBytesInt + SizeBytesInt + SizeBytesInt + SizeBytesInt +
			SizeBytesInt + SizeBytesFloat64 + SizeBytesFloat64 + SizeBytesFloat64 + AvgBytesStringTiny + AvgBytesStringShort, nil
	default:
		return 0, fmt.Errorf("unknown table type: %s", tableType)
	}
}

// CalculateRowCounts determines the target number of rows for each table based on a target size in GB.
func CalculateRowCounts(targetGB float64) (RowCounts, error) {
	if targetGB <= 0 {
		return RowCounts{}, fmt.Errorf("target size must be positive")
	}

	targetBytes := targetGB * 1024 * 1024 * 1024

	// Estimate size primarily based on the fact table, as it usually dominates
	avgOrderSizeBytes, err := estimateAvgRowSizeBytes("orderfact")
	if err != nil {
		// Should not happen with hardcoded types, but handle defensively
		return RowCounts{}, fmt.Errorf("failed to estimate order size: %w", err)
	}
	if avgOrderSizeBytes <= 0 {
		return RowCounts{}, fmt.Errorf("estimated average order size is zero or negative")
	}

	// Initial estimate for number of orders
	numOrdersF := targetBytes / float64(avgOrderSizeBytes)
	numOrders := int(math.Max(1.0, math.Round(numOrdersF))) // Ensure at least 1 order

	// Derive dimension counts based on ratios
	numCustomersF := float64(numOrders) / DefaultOrdersPerCustomerRatio
	numProductsF := float64(numOrders) / DefaultOrdersPerProductRatio
	numLocationsF := float64(numOrders) / DefaultOrdersPerLocationRatio

	// Ensure minimum counts for dimensions (at least 1 of each)
	counts := RowCounts{
		Orders:    numOrders,
		Customers: int(math.Max(1.0, math.Round(numCustomersF))),
		Products:  int(math.Max(1.0, math.Round(numProductsF))),
		Locations: int(math.Max(1.0, math.Round(numLocationsF))),
	}

	// Optional: Refine estimate slightly by considering dimension sizes?
	// This adds complexity. For now, relying on the dominant fact table size is often sufficient.
	// Example refinement idea (more complex):
	// avgCustSize, _ := estimateAvgRowSizeBytes("customer")
	// avgProdSize, _ := estimateAvgRowSizeBytes("product")
	// avgLocSize, _ := estimateAvgRowSizeBytes("location")
	// totalEstimatedBytes := float64(counts.Orders)*float64(avgOrderSizeBytes) +
	// 	float64(counts.Customers)*float64(avgCustSize) +
	// 	float64(counts.Products)*float64(avgProdSize) +
	// 	float64(counts.Locations)*float64(avgLocSize)
	// adjustmentRatio := targetBytes / totalEstimatedBytes
	// counts.Orders = int(math.Max(1.0, math.Round(float64(counts.Orders)*adjustmentRatio)))
	// // Recalculate dimension counts based on adjusted orders... or adjust all counts by ratio.

	return counts, nil
}
