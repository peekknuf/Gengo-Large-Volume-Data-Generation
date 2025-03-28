// cmd/simulate_facts.go
package cmd

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"reflect" // Needed for writing header and potentially records
	"sort"    // For cumulative weight search
	"strings"
	"time"

	"github.com/apache/arrow/go/v12/arrow"        // <<< Add
	"github.com/apache/arrow/go/v12/arrow/array"  // <<< Add
	"github.com/apache/arrow/go/v12/arrow/memory" // <<< Add
	"github.com/apache/arrow/go/v12/parquet"      // <<< Add

	// <<< Add
	"github.com/apache/arrow/go/v12/parquet/pqarrow" // <<< Add
	gf "github.com/brianvoe/gofakeit/v6"
	// No specific distribution library needed for cumulative weight method
)

// --- Static data for Order generation ---
var (
	orderStatuses  = []string{"Pending", "Processing", "Shipped", "Delivered", "Cancelled", "Returned"}
	paymentMethods = []string{"Credit Card", "PayPal", "Debit Card", "Bank Transfer", "Apple Pay", "Google Pay", "Stripe", "Venmo"}
)

// --- Weighted Sampler ---

type weightedSampler struct {
	ids               []int     // The original IDs
	cumulativeWeights []float64 // Cumulative weights [0..1]
	totalWeight       float64   // Sum of raw weights (for potential debugging/info)
}

// setupWeightedSampler creates a sampler that picks IDs based on a power-law-like weight (1/rank).
// Assumes input IDs are somewhat meaningful in their order (e.g., lower IDs might be older/more frequent).
func setupWeightedSampler(ids []int) (*weightedSampler, error) {
	n := len(ids)
	if n == 0 {
		return nil, fmt.Errorf("cannot create sampler from empty ID list")
	}

	sampler := &weightedSampler{
		ids:               make([]int, n),
		cumulativeWeights: make([]float64, n),
	}
	copy(sampler.ids, ids) // Copy IDs to avoid external modification issues

	var currentCumulative float64 = 0
	var totalRawWeight float64 = 0

	// Calculate weights (e.g., 1 / sqrt(rank+1) for a less extreme skew than 1/rank)
	// and cumulative weights
	for i := 0; i < n; i++ {
		// Assign weight - using 1/sqrt(index+1) for slightly less aggressive skew
		// Can be tuned: 1.0 / float64(i+1) for stronger skew
		rawWeight := 1.0 / math.Sqrt(float64(i+1))
		if rawWeight <= 0 { // Avoid zero/negative weights
			rawWeight = 1e-9 // A tiny positive number
		}
		totalRawWeight += rawWeight
		currentCumulative += rawWeight                   // Add raw weight first
		sampler.cumulativeWeights[i] = currentCumulative // Store cumulative *raw* weight temporarily
	}

	sampler.totalWeight = totalRawWeight

	// Normalize cumulative weights to the range [0, 1]
	if totalRawWeight > 0 {
		for i := 0; i < n; i++ {
			sampler.cumulativeWeights[i] /= totalRawWeight
		}
		// Ensure the last element is exactly 1.0 due to potential float inaccuracies
		sampler.cumulativeWeights[n-1] = 1.0
	} else {
		// Handle edge case of zero total weight (all raw weights were <= 0?)
		// Assign equal probability if calculation failed
		fmt.Println("Warning: Total raw weight is zero in sampler. Assigning equal probability.")
		for i := 0; i < n; i++ {
			sampler.cumulativeWeights[i] = float64(i+1) / float64(n)
		}
	}

	return sampler, nil
}

// Sample returns a randomly selected ID based on the pre-calculated weights.
func (s *weightedSampler) Sample() int {
	if len(s.ids) == 0 {
		// Should not happen if constructor checks, but return a zero value defensively
		return 0
	}
	if len(s.ids) == 1 {
		return s.ids[0] // Only one choice
	}

	// Generate a random float64 between 0.0 (inclusive) and 1.0 (exclusive)
	r := rand.Float64()

	// Find the index where r falls within the cumulative weight range
	// Use binary search (sort.SearchFloat64s) for efficiency on large lists
	index := sort.SearchFloat64s(s.cumulativeWeights, r)

	// sort.SearchFloat64s returns the index where r would be inserted.
	// If r is exactly one of the cumulative weights, it might point to the next bucket.
	// However, because r is < 1.0 and cumulativeWeights go up to 1.0,
	// the index should be valid (0 <= index < len(s.cumulativeWeights)).
	// If index == len, it implies r >= last cumulative weight (which should be 1.0),
	// this shouldn't happen with rand.Float64() which is < 1.0, but handle defensively.
	if index >= len(s.ids) {
		index = len(s.ids) - 1 // Use the last valid index
	}

	return s.ids[index]
}

// --- Order Generation and Writing ---

// generateAndWriteOrders generates the fact table data and writes it to the specified format.
// Currently implemented for CSV.
func generateAndWriteOrders(numOrders int, customerIDs []int, productInfo map[int]ProductDetails, productIDsForSampling []int, locationIDs []int, format string, outputDir string) (err error) { // Use named return
	fmt.Printf("Starting generation of %s orders...\n", addUnderscores(numOrders))

	if numOrders <= 0 {
		fmt.Println("No orders to generate.")
		return nil
	}
	if len(customerIDs) == 0 || len(productIDsForSampling) == 0 || len(locationIDs) == 0 {
		return fmt.Errorf("cannot generate orders: dimension ID lists are empty")
	}

	// --- Setup Samplers ---
	customerSampler, err := setupWeightedSampler(customerIDs)
	if err != nil {
		return fmt.Errorf("failed to set up customer sampler: %w", err)
	}
	productSampler, err := setupWeightedSampler(productIDsForSampling)
	if err != nil {
		return fmt.Errorf("failed to set up product sampler: %w", err)
	}

	// --- Setup File ---
	targetFilename := filepath.Join(outputDir, "fact_orders."+format)
	var file *os.File
	file, err = os.Create(targetFilename)
	if err != nil {
		return fmt.Errorf("failed to create fact orders file %s: %w", targetFilename, err)
	}
	// Defer file close check ONLY if error occurred before writer setup/close
	defer func() {
		if err != nil && file != nil {
			// Attempt cleanup on error path
			// fmt.Printf("Debug: Error occurred (%v), attempting orders file close in defer for %s\n", err, targetFilename) // Debug print
			_ = file.Close()
		}
	}()

	// --- Setup Writer/Encoder/Builder based on format ---
	var csvWriter *csv.Writer
	var jsonEncoder *json.Encoder
	var pqWriter *pqarrow.FileWriter
	var pqBuilder *array.RecordBuilder
	var pqSchema *arrow.Schema
	var pqPool memory.Allocator

	switch format {
	case "csv":
		csvWriter = csv.NewWriter(file)
		// Defer CSV flush only
		defer csvWriter.Flush()
		// Write CSV Header
		orderFactType := reflect.TypeOf(OrderFact{})
		numFields := orderFactType.NumField()
		headers := make([]string, numFields)
		for i := 0; i < numFields; i++ {
			field := orderFactType.Field(i)
			headerName := field.Name
			jsonTag := field.Tag.Get("json")
			if jsonTag != "" {
				parts := strings.Split(jsonTag, ",")
				if parts[0] != "" && parts[0] != "-" {
					headerName = parts[0]
				}
			} else {
				pqTag := field.Tag.Get("parquet")
				if pqTag != "" {
					parts := strings.Split(pqTag, ",")
					if parts[0] != "" && parts[0] != "-" {
						headerName = parts[0]
					}
				}
			}
			headers[i] = headerName
		}
		if err = csvWriter.Write(headers); err != nil {
			_ = file.Close() // Cleanup on error
			return fmt.Errorf("failed to write csv header for orders: %w", err)
		}

	case "json":
		jsonEncoder = json.NewEncoder(file)
		// No header, no specific defer needed for encoder

	case "parquet":
		pqPool = memory.NewGoAllocator()
		pqSchema, err = buildArrowSchema(reflect.TypeOf(OrderFact{}))
		if err != nil {
			return err
		} // Early return triggers file close defer
		props := parquet.NewWriterProperties( /* ... */ )
		arrowProps := pqarrow.NewArrowWriterProperties()
		pqWriter, err = pqarrow.NewFileWriter(pqSchema, file, props, arrowProps)
		if err != nil {
			return err
		} // Early return triggers file close defer

		// *** Use DEFER for pqWriter close ***
		defer func() {
			// fmt.Printf("Debug: Running pqWriter.Close() defer for %s\n", targetFilename) // Debug print
			if closeErr := pqWriter.Close(); closeErr != nil && err == nil {
				// fmt.Printf("Debug: pqWriter.Close() returned error: %v for %s\n", closeErr, targetFilename) // Debug print
				err = fmt.Errorf("error closing orders parquet writer: %w", closeErr)
			}
			// fmt.Printf("Debug: Finished pqWriter.Close() defer for %s. Final err: %v\n", targetFilename, err) // Debug print
		}()

		pqBuilder = array.NewRecordBuilder(pqPool, pqSchema)
		// Defer builder release (runs BEFORE writer close)
		defer pqBuilder.Release()

	default:
		return fmt.Errorf("unsupported format: '%s'", format) // Early return triggers file close defer
	}

	// --- Generation Loop ---
	var record []string
	var orderFactType reflect.Type
	var numFields int
	var fieldIndices []int
	var pqFieldMap map[string]int

	if format == "csv" { /* ... setup CSV vars ... */
		orderFactType = reflect.TypeOf(OrderFact{})
		numFields = orderFactType.NumField()
		record = make([]string, numFields)
		fieldIndices = make([]int, numFields)
		for i := 0; i < numFields; i++ {
			fieldIndices[i] = i
		}
	} else if format == "parquet" { /* ... setup Parquet map ... */
		orderFactType = reflect.TypeOf(OrderFact{})
		pqFieldMap = map[string]int{}
		for i := 0; i < orderFactType.NumField(); i++ {
			field := orderFactType.Field(i)
			name := getParquetFieldName(field)
			if name != "-" {
				pqFieldMap[name] = i
			}
		}
	}

	gf.Seed(time.Now().UnixNano())
	startTime := time.Now().AddDate(-5, 0, 0)
	endTime := time.Now()
	fmt.Printf("Generating and writing orders to %s...\n", targetFilename)
	progressStep := numOrders / 20
	if progressStep == 0 {
		progressStep = 1
	}
	rowsInCurrentBatch := 0

	for i := 0; i < numOrders; i++ {
		// --- Generate FKs & Details (unchanged) ---
		customerID := customerSampler.Sample()
		productID := productSampler.Sample()
		shippingLocID := locationIDs[rand.Intn(len(locationIDs))]
		billingLocID := shippingLocID
		if rand.Intn(100) < 20 {
			billingLocID = locationIDs[rand.Intn(len(locationIDs))]
		}
		quantity := gf.Number(1, 15)
		orderStatus := orderStatuses[rand.Intn(len(orderStatuses))]
		paymentMethod := paymentMethods[rand.Intn(len(paymentMethods))]
		orderTimestamp := gf.DateRange(startTime, endTime)
		discount := 0.0
		if rand.Intn(100) < 30 {
			discount = gf.Float64Range(0.05, 0.25)
		}
		details, ok := productInfo[productID]
		if !ok {
			fmt.Fprintf(os.Stderr, "Warning: ProductID %d not found. Skipping order %d.\n", productID, i+1)
			continue
		}
		unitPrice := details.BasePrice
		totalPrice := float64(quantity) * unitPrice * (1.0 - discount)

		// --- Create Struct (unchanged) ---
		order := OrderFact{ /* ... fields ... */
			OrderID: i + 1, OrderTimestamp: orderTimestamp, CustomerID: customerID, ProductID: productID,
			ShippingLocationID: shippingLocID, BillingLocationID: billingLocID, Quantity: quantity,
			UnitPrice: unitPrice, Discount: discount, TotalPrice: totalPrice, OrderStatus: orderStatus,
			PaymentMethod: paymentMethod,
		}
		orderVal := reflect.ValueOf(order)

		// --- Write Record ---
		switch format {
		case "csv":
			for j := 0; j < numFields; j++ {
				record[j] = valueToString(orderVal.Field(fieldIndices[j]))
			}
			if writeErr := csvWriter.Write(record); writeErr != nil {
				err = fmt.Errorf("failed to write order record %d to csv: %w", i+1, writeErr)
				_ = file.Close() // Cleanup on error
				return err
			}
		case "json":
			if encodeErr := jsonEncoder.Encode(order); encodeErr != nil {
				err = fmt.Errorf("failed to encode order record %d to json: %w", i+1, encodeErr)
				_ = file.Close() // Cleanup on error
				return err
			}
		case "parquet":
			for fieldIdx, arrowField := range pqSchema.Fields() {
				structFieldIndex, ok := pqFieldMap[arrowField.Name]
				if !ok {
					err = fmt.Errorf("schema field '%s' not found in map", arrowField.Name)
					_ = pqWriter.Close()
					_ = file.Close()
					return err
				}
				fieldVal := orderVal.Field(structFieldIndex)
				if appendErr := appendValueToBuilder(pqBuilder.Field(fieldIdx), fieldVal); appendErr != nil {
					err = fmt.Errorf("append field %s record %d: %w", arrowField.Name, i+1, appendErr)
					_ = pqWriter.Close()
					_ = file.Close()
					return err
				}
			}
			rowsInCurrentBatch++
			if rowsInCurrentBatch >= parquetWriteBatchSize {
				newBuilder, writeErr := writeParquetBatchCorrected(pqWriter, pqBuilder, pqPool, pqSchema, targetFilename)
				if writeErr != nil {
					err = writeErr
					return err
				} // Early return triggers defers
				pqBuilder = newBuilder
				rowsInCurrentBatch = 0
			}
		} // End write switch

		// --- Progress Update ---
		if (i+1)%progressStep == 0 || i == numOrders-1 {
			fmt.Printf("... %s / %s orders processed\n", addUnderscores(i+1), addUnderscores(numOrders))
		}
	} // End order loop

	// --- Final Actions After Loop ---
	switch format {
	case "csv":
		// Flush is handled by defer. Check error.
		if flushErr := csvWriter.Error(); flushErr != nil && err == nil {
			err = fmt.Errorf("error during final order csv flushing: %w", flushErr)
			// File close is handled by its defer
		}
	case "parquet":
		// Write final batch
		if rowsInCurrentBatch > 0 {
			_, writeErr := writeParquetBatchCorrected(pqWriter, pqBuilder, pqPool, pqSchema, targetFilename)
			if writeErr != nil && err == nil {
				err = writeErr
				return err
			} // Early return triggers defers
		}
	case "json":
		// File close is handled by defer. Nothing specific needed here.
		break

	} // End final actions switch

	// --- Return Final Error State ---
	// File close for CSV/JSON is handled by the top-level defer. Check its error if needed.
	if err == nil && (format == "csv" || format == "json") {
		// Check file close error from defer if needed, logic is already there.
	}

	if err == nil {
		fmt.Printf("Successfully finished writing %s orders to %s\n", addUnderscores(numOrders), targetFilename)
	}
	// The named return 'err' holds the first error encountered or nil
	return err
}
