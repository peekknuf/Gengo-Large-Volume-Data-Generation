package cmd

import (
	"fmt"
	gf "github.com/brianvoe/gofakeit/v6"
	"strings"
	"sync"
	"time"
)

type Row struct {
	ID          int
	Timestamp   time.Time
	ProductName string
	Company     string
	Price       float64
	Quantity    int
	Discount    float64
	TotalPrice  float64
	FirstName   string
	LastName    string
	Email       string
	Address     string
	City        string
	State       string
	Zip         string
	Country     string
}

func simulatingData(numRows int, selectedCols []string, wg *sync.WaitGroup, ch chan<- Row) {
	defer wg.Done()

	timing := time.Now()

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
				row.Price = gf.Price(1.99, 399.99)
			case "Quantity":
				row.Quantity = gf.Number(1, 499)
			case "Discount":
				row.Discount = gf.Float64Range(0.0, 0.66)
			case "FirstName":
				row.FirstName = gf.FirstName()
			case "LastName":
				row.LastName = gf.LastName()
			case "Email":
				row.Email = gf.Email()
				if !strings.Contains(row.Email, "gmail.com") {
					row.Email = ""
				}
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
