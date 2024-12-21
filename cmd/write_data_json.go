package cmd

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"
)

func WriteToJSON(filename string, ch <-chan Row, wg *sync.WaitGroup, selectedCols []string) {
	defer wg.Done()

	file, err := os.Create(filename)
	if err != nil {
		fmt.Println("Error creating file:", err)
		return
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ") // pretty print JSON

	for row := range ch {
		record := make(map[string]interface{})
		for _, col := range selectedCols {
			switch col {
			case "ID":
				record["ID"] = row.ID
			case "Timestamp":
				record["Timestamp"] = row.Timestamp.Format(time.RFC3339)
			case "ProductName":
				record["ProductName"] = row.ProductName
			case "Company":
				record["Company"] = row.Company
			case "Price":
				record["Price"] = row.Price
			case "Quantity":
				record["Quantity"] = row.Quantity
			case "Discount":
				record["Discount"] = row.Discount
			case "TotalPrice":
				record["TotalPrice"] = row.TotalPrice
			case "FirstName":
				record["FirstName"] = row.FirstName
			case "LastName":
				record["LastName"] = row.LastName
			case "Email":
				record["Email"] = row.Email
			case "Address":
				record["Address"] = row.Address
			case "City":
				record["City"] = row.City
			case "State":
				record["State"] = row.State
			case "Zip":
				record["Zip"] = row.Zip
			case "Country":
				record["Country"] = row.Country
			case "OrderStatus":
				record["OrderStatus"] = row.OrderStatus
			case "PaymentMethod":
				record["PaymentMethod"] = row.PaymentMethod
			case "ShippingAddress":
				record["ShippingAddress"] = row.ShippingAddress
			case "ProductCategory":
				record["ProductCategory"] = row.ProductCategory
			default:
				fmt.Printf("Unknown column: %s\n", col)
			}
		}
		if err := encoder.Encode(record); err != nil {
			fmt.Println("Error writing record:", err)
			return
		}
	}
}
