package gen

import (
	"fmt"
	"sync"
	"time"

	gf "github.com/brianvoe/gofakeit/v6"
)

// Row represents a data row.
type Row struct {
	ID          int
	Timestamp   time.Time
	ProductName string
	Company     string
	Price       float64
	Quantity    int
	Discount    float64
	TotalPrice  float64
	CustomerID  int
	FirstName   string
	LastName    string
	Email       string
	Address     string
	City        string
	State       string
	Zip         string
	Country     string
}

// GenerateData generates data and sends it to the provided channel.
func GenerateData(numRows int, selectedCols []string, wg *sync.WaitGroup, ch chan<- Row) {
	defer wg.Done()

	startTime := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	endTime := time.Now()

	for i := 0; i < numRows; i++ {
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
				row.Price = gf.Price(4.99, 399.99)
			case "Quantity":
				row.Quantity = gf.Number(1, 50)
			case "Discount":
				row.Discount = gf.Float64Range(0.0, 0.66)
			case "TotalPrice":
				price := gf.Price(4.99, 399.99)
				discount := gf.Float64Range(0.0, 0.66)
				row.TotalPrice = price * (1 - discount)
			case "CustomerID":
				row.CustomerID = gf.Number(1, 99999)
			case "FirstName":
				row.FirstName = gf.FirstName()
			case "LastName":
				row.LastName = gf.LastName()
			case "Email":
				row.Email = gf.Email()
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
				}
			default:
				fmt.Printf("Unknown column: %s\n", col)
			}
		}

		ch <- row
	}

	close(ch)

	elapsedTime := time.Since(startTime)
	fmt.Printf("Data generation took %s\n", elapsedTime)
}
