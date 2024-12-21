package cmd

import (
	"fmt"
	"math/rand"
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

	timing := time.Now()

	// Create a normal distribution for price
	priceDist := distuv.Normal{
		Mu:    100,
		Sigma: 50,
	}

	// Create a random number generator
	rand.New(rand.NewSource(time.Now().UnixNano()))

	// Create a list of common email providers
	emailProviders := []string{"gmail.com", "yahoo.com", "outlook.com"}

	// Create a list of common product categories
	productCategories := []string{"electronics", "clothing", "home goods"}

	// Create a list of order statuses
	orderStatuses := []string{"pending", "shipped", "delivered"}

	// Create a list of payment methods
	paymentMethods := []string{"credit card", "paypal", "bank transfer"}

	for i := 0; i < numRows; i++ {
		startTime := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
		endTime := time.Now()

		row := Row{}

		for _, col := range selectedCols {
			switch col {
			case "ID":
				row.ID = i + 1
			case "Timestamp":
				row.Timestamp = gf.DateRange(startTime, endTime)
			case "ProductName":
				row.ProductName = gf.CarModel()
			case "Company":
				row.Company = gf.Company()
			case "Price":
				row.Price = priceDist.Rand()
			case "Quantity":
				row.Quantity = gf.Number(1, 499)
			case "Discount":
				row.Discount = gf.Float64Range(0.0, 0.66)
			case "FirstName":
				row.FirstName = gf.FirstName()
			case "LastName":
				row.LastName = gf.LastName()
			case "Email":
				emailProvider := emailProviders[rand.Intn(len(emailProviders))]
				row.Email = fmt.Sprintf("%s@%s", gf.Username(), emailProvider)
			case "Address":
				row.Address = gf.Address().Address
			case "City":
				row.City = gf.City()
			case "State":
				row.State = gf.State()
			case "Zip":
				row.Zip = gf.Zip()
			case "Country":
				row.Country = gf.Country()
				if row.Country != "United States of America" {
					row.State = ""
					row.City = ""
					row.Address = ""
				}
			case "OrderStatus":
				row.OrderStatus = orderStatuses[rand.Intn(len(orderStatuses))]
			case "PaymentMethod":
				row.PaymentMethod = paymentMethods[rand.Intn(len(paymentMethods))]
			case "ShippingAddress":
				row.ShippingAddress = gf.Address().Address
			case "ProductCategory":
				row.ProductCategory = productCategories[rand.Intn(len(productCategories))]
			default:
				fmt.Printf("Unknown column: %s\n", col)
			}
		}

		ch <- row
	}

	close(ch)

	elapsedTime := time.Since(timing).Seconds()
	fmt.Printf("Data generation took %.2f s\n", elapsedTime)
}
