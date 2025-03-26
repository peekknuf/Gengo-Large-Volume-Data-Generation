package cmd

import (
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"

	gf "github.com/brianvoe/gofakeit/v6"
	"gonum.org/v1/gonum/stat/distuv"
)

type Row struct {
	ID              int
	Timestamp       time.Time
	ProductName     string
	Company         string
	Price           float64
	Quantity        int
	Discount        float64
	TotalPrice      float64
	FirstName       string
	LastName        string
	Email           string
	Address         string
	City            string
	State           string
	Zip             string
	Country         string
	OrderStatus     string
	PaymentMethod   string
	ShippingAddress string
	ProductCategory string
}

func simulatingData(numRows int, selectedCols []string, wg *sync.WaitGroup, ch chan<- Row) {
	defer wg.Done()
	defer close(ch) // Close channel when producer is done

	timing := time.Now()
	gf.Seed(time.Now().UnixNano()) // Seed gofakeit

	// Create distributions and static lists
	priceDist := distuv.Normal{Mu: 100, Sigma: 50}
	emailProviders := []string{"gmail.com", "yahoo.com", "outlook.com", "aol.com", "protonmail.com"}
	productCategories := []string{"Electronics", "Clothing", "Home Goods", "Books", "Sports", "Toys", "Grocery"}
	orderStatuses := []string{"Pending", "Processing", "Shipped", "Delivered", "Cancelled", "Returned"}
	paymentMethods := []string{"Credit Card", "PayPal", "Debit Card", "Bank Transfer", "Apple Pay", "Google Pay"}

	for i := 0; i < numRows; i++ {
		startTime := time.Date(2010, 1, 1, 0, 0, 0, 0, time.UTC)
		endTime := time.Now()

		row := Row{ID: i + 1} // Assign ID immediately

		// --- Pre-generate Address Components (Always run now) ---
		var genAddress, genCity, genState, genZip, genCountry, genShippingAddress string

		// Decide country first (e.g., 80% chance US for more realistic gofakeit data)
		isUS := rand.Intn(100) < 80
		if isUS {
			addr := gf.Address() // Use the detailed US generator
			genAddress = addr.Address
			genCity = addr.City
			genState = addr.State
			genZip = addr.Zip
			genCountry = addr.Country                 // Should be "United States"
			genShippingAddress = gf.Address().Address // Generate a separate (potentially different) US shipping address
		} else {
			// Non-US Address Generation (more generic)
			genCountry = gf.Country()
			// Ensure we didn't randomly get US again
			for genCountry == "United States" || genCountry == "United States of America" {
				genCountry = gf.Country()
			}
			genCity = gf.City()                                   // Use generic city - may still sound US-like
			genState = ""                                         // States less common/structured globally
			genZip = gf.Numerify("#####")                         // Generic 5-digit postal code
			genAddress = gf.StreetNumber() + "" + gf.StreetName() // Generic street address
			genShippingAddress = gf.StreetNumber() + "" + gf.StreetName()
		}
		// --- End Address Pre-generation ---

		// --- Assign values for all selected columns ---
		// We assume 'selectedCols' contains all the columns we handle here.
		for _, col := range selectedCols {
			switch col {
			// ID is handled at row creation
			case "Timestamp":
				row.Timestamp = gf.DateRange(startTime, endTime)
			case "ProductName":
				row.ProductName = gf.Product().Name
			case "Company":
				row.Company = gf.Company()
			case "Price":
				price := priceDist.Rand()
				if price < 1.0 {
					price = gf.Float64Range(1.0, 20.0)
				}
				row.Price = price
			case "Quantity":
				row.Quantity = gf.Number(1, 15)
			case "Discount":
				if rand.Intn(10) < 3 {
					row.Discount = gf.Float64Range(0.05, 0.30)
				} else {
					row.Discount = 0.0
				}
			// case "TotalPrice": // Handled after the loop if included
			case "FirstName":
				row.FirstName = gf.FirstName()
			case "LastName":
				row.LastName = gf.LastName()
			case "Email":
				emailProvider := emailProviders[rand.Intn(len(emailProviders))]
				row.Email = fmt.Sprintf("%s.%s%d@%s", strings.ToLower(gf.LetterN(5)), strings.ToLower(gf.LetterN(4)), gf.Number(1, 99), emailProvider)

			// --- Address Components Assignment ---
			case "Address":
				row.Address = genAddress
			case "City":
				row.City = genCity
			case "State":
				row.State = genState
			case "Zip":
				row.Zip = genZip
			case "Country":
				row.Country = genCountry
			case "ShippingAddress":
				row.ShippingAddress = genShippingAddress
			// --- End Address Components ---

			case "OrderStatus":
				row.OrderStatus = orderStatuses[rand.Intn(len(orderStatuses))]
			case "PaymentMethod":
				row.PaymentMethod = paymentMethods[rand.Intn(len(paymentMethods))]
			case "ProductCategory":
				row.ProductCategory = productCategories[rand.Intn(len(productCategories))]

			default:
				// If selectedCols *could* contain unknown columns in the future, keep warning
				// fmt.Printf("Warning: Unknown or unhandled column in simulatingData: %s\n", col)
				break // Ignore unknown/unhandled columns for now
			}
		}

		// --- Calculate TotalPrice (if it's in the hardcoded selectedCols list) ---
		// Assuming TotalPrice might be added/removed from the global selectedCols variable
		shouldCalcTotalPrice := false
		for _, c := range selectedCols {
			if c == "TotalPrice" {
				shouldCalcTotalPrice = true
				break
			}
		}
		if shouldCalcTotalPrice {
			row.TotalPrice = row.Price * float64(row.Quantity) * (1 - row.Discount)
		}

		ch <- row
	} // End row loop

	elapsedTime := time.Since(timing).Seconds()
	fmt.Printf("\nData generation finished in %.2f seconds.\n", elapsedTime)
}
