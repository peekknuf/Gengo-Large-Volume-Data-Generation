package gen

import (
	"encoding/csv"
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"
)

func WriteToCSV(filename string, ch <-chan Row, wg *sync.WaitGroup, selectedCols []string) {
	defer wg.Done()

	file, err := os.Create(filename)
	if err != nil {
		fmt.Println("Error creating file:", err)
		return
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	
	if err := writer.Write(selectedCols); err != nil {
		fmt.Println("Error writing header:", err)
		return
	}

	for row := range ch {
		record := make([]string, len(selectedCols))
		for i, col := range selectedCols {
			switch col {
			case "ID":
				record[i] = strconv.Itoa(row.ID)
			case "Timestamp":
				record[i] = row.Timestamp.Format(time.RFC3339)
			case "ProductName":
				record[i] = row.ProductName
			case "Company":
				record[i] = row.Company
			case "Price":
				record[i] = fmt.Sprintf("%.2f", row.Price)
			case "Quantity":
				record[i] = strconv.Itoa(row.Quantity)
			case "Discount":
				record[i] = fmt.Sprintf("%.2f", row.Discount)
			case "TotalPrice":
				record[i] = fmt.Sprintf("%.2f", row.TotalPrice)
			case "CustomerID":
				record[i] = strconv.Itoa(row.CustomerID)
			case "FirstName":
				record[i] = row.FirstName
			case "LastName":
				record[i] = row.LastName
			case "Email":
				record[i] = row.Email
			case "Address":
				record[i] = row.Address
			case "City":
				record[i] = row.City
			case "State":
				record[i] = row.State
			case "Zip":
				record[i] = row.Zip
			case "Country":
				record[i] = row.Country
			}
		}
		if err := writer.Write(record); err != nil {
			fmt.Println("Error writing record:", err)
			return
		}
	}
}
