// cmd/simulate_dims.go
package cmd

import (
	"fmt"
	"math/rand"
	"strings"

	gf "github.com/brianvoe/gofakeit/v6"
	"gonum.org/v1/gonum/stat/distuv" // Keep using this for prices etc.
)

// --- Static Data for Generators ---
// (Can be expanded or made configurable later)
var (
	emailProviders    = []string{"gmail.com", "yahoo.com", "outlook.com", "aol.com", "protonmail.com", "hey.com"}
	productCategories = []string{"Electronics", "Clothing", "Home Goods", "Books", "Sports", "Toys", "Grocery", "Automotive", "Health", "Beauty"}
)

// generateCustomers creates a slice of Customer structs.
func generateCustomers(count int) []Customer {
	if count <= 0 {
		return []Customer{}
	}
	customers := make([]Customer, count)
	fmt.Printf("Generating %d customers...\n", count) // Progress indicator

	for i := 0; i < count; i++ {
		firstName := gf.FirstName()
		lastName := gf.LastName()
		emailProvider := emailProviders[rand.Intn(len(emailProviders))]
		// Generate a slightly more realistic-looking email
		email := fmt.Sprintf("%s.%s%d@%s",
			strings.ToLower(firstName),
			strings.ToLower(lastName),
			gf.Number(1, 999), // Add some numbers
			emailProvider)

		customers[i] = Customer{
			CustomerID: i + 1, // Simple sequential ID starting from 1
			FirstName:  firstName,
			LastName:   lastName,
			Email:      gf.Numerify(email), // Ensure it looks plausible if names have numbers/symbols
		}
		// Add progress feedback for large counts?
		// if i% (count/10) == 0 && i > 0 { // Example: print every 10%
		// 	fmt.Printf("... generated %d customers\n", i)
		// }
	}
	return customers
}

// generateProducts creates a slice of Product structs.
func generateProducts(count int) []Product {
	if count <= 0 {
		return []Product{}
	}
	products := make([]Product, count)
	fmt.Printf("Generating %d products...\n", count)

	// Use a distribution for more realistic pricing
	// Adjust Mu (mean) and Sigma (std dev) as needed
	priceDist := distuv.Normal{Mu: 75, Sigma: 45}

	for i := 0; i < count; i++ {
		category := productCategories[rand.Intn(len(productCategories))]
		// Use gofakeit's product generator, maybe tailor by category later?
		productName := gf.ProductName() // gf.Product().Name is also an option

		// Generate a price, ensure it's not negative or unrealistically low
		price := priceDist.Rand()
		if price < 5.0 { // Set a minimum reasonable price
			price = gf.Float64Range(5.0, 25.0)
		}

		products[i] = Product{
			ProductID:       i + 1, // Simple sequential ID
			ProductName:     productName,
			ProductCategory: category,
			Company:         gf.Company(),
			BasePrice:       price, // Use the generated price
		}
	}
	return products
}

// generateLocations creates a slice of Location structs.
func generateLocations(count int) []Location {
	if count <= 0 {
		return []Location{}
	}
	locations := make([]Location, count)
	fmt.Printf("Generating %d locations...\n", count)

	for i := 0; i < count; i++ {
		var addr, city, state, zip, country string

		// Reuse the US/non-US logic from the original simulateData
		isUS := rand.Intn(100) < 80 // 80% chance US
		if isUS {
			addrInfo := gf.Address()
			addr = addrInfo.Address
			city = addrInfo.City
			state = addrInfo.State
			zip = addrInfo.Zip
			country = addrInfo.Country // Should be "United States" or similar
			if country != "United States" && country != "United States of America" {
				// Gofakeit sometimes returns slightly different names, normalize
				country = "United States"
			}
		} else {
			country = gf.Country()
			for country == "United States" || country == "United States of America" {
				country = gf.Country() // Ensure non-US
			}
			city = gf.City()
			state = "" // Less common/structured globally
			// Generate a postal code appropriate for the country? Hard. Use generic for now.
			zip = gf.Numerify("#####")                                                  // Generic 5-digit or range
			addr = gf.StreetNumber() + " " + gf.StreetName() + ", " + gf.StreetSuffix() // More generic street
		}

		locations[i] = Location{
			LocationID: i + 1, // Simple sequential ID
			Address:    addr,
			City:       city,
			State:      state,
			Zip:        zip,
			Country:    country,
		}
	}
	return locations
}
