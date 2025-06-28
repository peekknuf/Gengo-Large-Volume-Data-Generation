// cmd/simulate_dims.go
package cmd

import (
	"fmt"
	"math/rand"
	"strings"

	gf "github.com/brianvoe/gofakeit/v6"
	"gonum.org/v1/gonum/stat/distuv"
)

// --- Static Data for Generators ---
var (
	emailProviders    = []string{"gmail.com", "yahoo.com", "outlook.com", "aol.com", "protonmail.com", "hey.com"}
	productCategories = []string{"Electronics", "Clothing", "Home Goods", "Books", "Sports", "Toys", "Grocery", "Automotive", "Health", "Beauty"}
	addressTypes      = []string{"shipping", "billing"}
)

// generateCustomers creates a slice of Customer structs.
func generateCustomers(count int) []Customer {
	if count <= 0 {
		return []Customer{}
	}
	customers := make([]Customer, count)
	for i := 0; i < count; i++ {
		firstName := gf.FirstName()
		lastName := gf.LastName()
		emailProvider := emailProviders[rand.Intn(len(emailProviders))]
		email := fmt.Sprintf("%s.%s%d@%s",
			strings.ToLower(firstName),
			strings.ToLower(lastName),
			gf.Number(1, 999),
			emailProvider)

		customers[i] = Customer{
			CustomerID: i + 1,
			FirstName:  firstName,
			LastName:   lastName,
			Email:      gf.Numerify(email),
		}
	}
	return customers
}

func generateCustomerAddresses(customers []Customer) []CustomerAddress {
	if len(customers) == 0 {
		return []CustomerAddress{}
	}
	addresses := make([]CustomerAddress, 0)
	addressIDCounter := 1

	for _, customer := range customers {
		numAddresses := rand.Intn(3) + 1 // 1 to 3 addresses per customer
		for j := 0; j < numAddresses; j++ {
			addrInfo := gf.Address()
			addressType := addressTypes[rand.Intn(len(addressTypes))]

			addresses = append(addresses, CustomerAddress{
				AddressID:   addressIDCounter,
				CustomerID:  customer.CustomerID,
				AddressType: addressType,
				Address:     addrInfo.Address,
				City:        addrInfo.City,
				State:       addrInfo.State,
				Zip:         addrInfo.Zip,
				Country:     "United States", // Simplified for now
			})
			addressIDCounter++
		}
	}
	return addresses
}

func generateSuppliers(count int) []Supplier {
	if count <= 0 {
		return []Supplier{}
	}
	suppliers := make([]Supplier, count)
	for i := 0; i < count; i++ {
		suppliers[i] = Supplier{
			SupplierID:   i + 1,
			SupplierName: gf.Company(),
			Country:      gf.Country(),
		}
	}
	return suppliers
}

func generateProducts(count int, supplierIDs []int) []Product {
	if count <= 0 || len(supplierIDs) == 0 {
		return []Product{}
	}
	products := make([]Product, count)

	priceDist := distuv.Normal{Mu: 75, Sigma: 45}

	for i := 0; i < count; i++ {
		category := productCategories[rand.Intn(len(productCategories))]
		productName := gf.ProductName()
		price := priceDist.Rand()
		if price < 5.0 {
			price = gf.Float64Range(5.0, 25.0)
		}

		products[i] = Product{
			ProductID:       i + 1,
			SupplierID:      supplierIDs[rand.Intn(len(supplierIDs))],
			ProductName:     productName,
			ProductCategory: category,
			BasePrice:       price,
		}
	}
	return products
}
