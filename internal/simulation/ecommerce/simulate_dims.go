package ecommerce

import (
	"fmt"
	"math"
	"strings"

	"github.com/peekknuf/Gengo/internal/generation"
	"github.com/peekknuf/Gengo/internal/models/ecommerce"
)

var (
	emailProviders    = []string{"gmail.com", "yahoo.com", "outlook.com", "aol.com", "protonmail.com", "hey.com"}
	productCategories = []string{"Electronics", "Clothing", "Home Goods", "Books", "Sports", "Toys", "Grocery", "Automotive", "Health", "Beauty"}
	addressTypes      = []string{"shipping", "billing"}
)

func GenerateCustomers(count int, options ...generation.Config) []ecommerce.Customer {
	cfg := generation.Options(options)
	rng := cfg.Rand("ecommerce.GenerateCustomers", 0)
	fake := cfg.Faker("ecommerce.GenerateCustomers")
	if count <= 0 {
		return []ecommerce.Customer{}
	}
	customers := make([]ecommerce.Customer, count)
	for i := 0; i < count; i++ {
		firstName := fake.FirstName()
		lastName := fake.LastName()
		emailProvider := emailProviders[rng.Intn(len(emailProviders))]
		email := fmt.Sprintf("%s.%s%d@%s",
			strings.ToLower(firstName),
			strings.ToLower(lastName),
			fake.Number(1, 999),
			emailProvider)

		customers[i] = ecommerce.Customer{
			CustomerID: i + 1,
			FirstName:  firstName,
			LastName:   lastName,
			Email:      fake.Numerify(email),
		}
	}
	return customers
}

func GenerateCustomerAddresses(customers []ecommerce.Customer, count int, options ...generation.Config) []ecommerce.CustomerAddress {
	cfg := generation.Options(options)
	rng := cfg.Rand("ecommerce.GenerateCustomerAddresses", 0)
	fake := cfg.Faker("ecommerce.GenerateCustomerAddresses")
	if len(customers) == 0 {
		return []ecommerce.CustomerAddress{}
	}
	addresses := make([]ecommerce.CustomerAddress, 0, count)
	addressIDCounter := 1

	for i, customer := range customers {
		numAddresses := childCount(count-len(addresses), len(customers)-i-1, 3, rng.Intn)
		for j := 0; j < numAddresses; j++ {
			addrInfo := fake.Address()
			addressType := addressTypes[rng.Intn(len(addressTypes))]

			addresses = append(addresses, ecommerce.CustomerAddress{
				AddressID:   addressIDCounter,
				CustomerID:  customer.CustomerID,
				AddressType: addressType,
				Address:     addrInfo.Address,
				City:        addrInfo.City,
				State:       addrInfo.State,
				Zip:         addrInfo.Zip,
				Country:     "United States",
			})
			addressIDCounter++
		}
	}
	return addresses
}

func GenerateSuppliers(count int, options ...generation.Config) []ecommerce.Supplier {
	cfg := generation.Options(options)
	fake := cfg.Faker("ecommerce.GenerateSuppliers")
	if count <= 0 {
		return []ecommerce.Supplier{}
	}
	suppliers := make([]ecommerce.Supplier, count)
	for i := 0; i < count; i++ {
		suppliers[i] = ecommerce.Supplier{
			SupplierID:   i + 1,
			SupplierName: fake.Company(),
			Country:      fake.Country(),
		}
	}
	return suppliers
}

func GenerateProductCategories() []ecommerce.ProductCategory {
	categories := make([]ecommerce.ProductCategory, len(productCategories))
	for i, name := range productCategories {
		categories[i] = ecommerce.ProductCategory{
			CategoryID:   i + 1,
			CategoryName: name,
		}
	}
	return categories
}

func GenerateProducts(count int, supplierIDs []int, categoryIDs []int, options ...generation.Config) []ecommerce.Product {
	cfg := generation.Options(options)
	rng := cfg.Rand("ecommerce.GenerateProducts", 0)
	fake := cfg.Faker("ecommerce.GenerateProducts")
	if count <= 0 || len(supplierIDs) == 0 {
		return []ecommerce.Product{}
	}
	products := make([]ecommerce.Product, count)

	for i := 0; i < count; i++ {
		productName := fake.ProductName()
		price := 75 + 45*rng.NormFloat64()
		if price < 5.0 {
			price = fake.Float64Range(5.0, 25.0)
		}

		products[i] = ecommerce.Product{
			ProductID:   i + 1,
			SupplierID:  supplierIDs[rng.Intn(len(supplierIDs))],
			ProductName: productName,
			CategoryID:  categoryIDs[rng.Intn(len(categoryIDs))],
			BasePrice:   math.Round(price*100) / 100,
		}
	}
	return products
}
