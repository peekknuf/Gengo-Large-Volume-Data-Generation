package formats

import (
	"encoding/csv"
	"fmt"
	"os"
	"strconv"
	"time"

	ecommercemodels "github.com/peekknuf/Gengo/internal/models/ecommerce"
	financialmodels "github.com/peekknuf/Gengo/internal/models/financial"
	medicalmodels "github.com/peekknuf/Gengo/internal/models/medical"
)

// --- High-Performance, Type-Specific CSV Writers for E-commerce ---

// WriteCustomersToCSV writes a slice of Customer structs to a CSV file using a streaming approach.
func WriteCustomersToCSV(customers []ecommercemodels.Customer, targetFilename string) error {
	file, err := os.Create(targetFilename)
	if err != nil {
		return fmt.Errorf("failed to create csv file %s: %w", targetFilename, err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	headers := []string{"customer_id", "first_name", "last_name", "email"}
	if err := writer.Write(headers); err != nil {
		return fmt.Errorf("failed to write csv header to %s: %w", targetFilename, err)
	}

	record := make([]string, len(headers))
	for _, c := range customers {
		record[0] = strconv.Itoa(c.CustomerID)
		record[1] = c.FirstName
		record[2] = c.LastName
		record[3] = c.Email
		if err := writer.Write(record); err != nil {
			return fmt.Errorf("failed to write csv record to %s: %w", targetFilename, err)
		}
	}

	if err := writer.Error(); err != nil {
		return fmt.Errorf("error occurred during csv writing to %s: %w", targetFilename, err)
	}

	fmt.Printf("Successfully wrote %d records to %s\n", len(customers), targetFilename)
	return nil
}

func WriteDailyStockPricesToCSV(prices []financialmodels.DailyStockPrice, targetFilename string) error {
	file, err := os.Create(targetFilename)
	if err != nil {
		return fmt.Errorf("failed to create csv file %s: %w", targetFilename, err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	headers := []string{"price_id", "date", "company_id", "exchange_id", "open_price", "high_price", "low_price", "close_price", "volume"}
	if err := writer.Write(headers); err != nil {
		return fmt.Errorf("failed to write csv header to %s: %w", targetFilename, err)
	}

	record := make([]string, len(headers))
	for _, p := range prices {
		record[0] = strconv.FormatInt(p.PriceID, 10)
		record[1] = p.Date.Format("2006-01-02")
		record[2] = strconv.Itoa(p.CompanyID)
		record[3] = strconv.Itoa(p.ExchangeID)
		record[4] = strconv.FormatFloat(p.OpenPrice, 'f', 4, 64)
		record[5] = strconv.FormatFloat(p.HighPrice, 'f', 4, 64)
		record[6] = strconv.FormatFloat(p.LowPrice, 'f', 4, 64)
		record[7] = strconv.FormatFloat(p.ClosePrice, 'f', 4, 64)
		record[8] = strconv.Itoa(p.Volume)
		if err := writer.Write(record); err != nil {
			return fmt.Errorf("failed to write csv record to %s: %w", targetFilename, err)
		}
	}

	if err := writer.Error(); err != nil {
		return fmt.Errorf("error occurred during csv writing to %s: %w", targetFilename, err)
	}

	fmt.Printf("Successfully wrote %d records to %s\n", len(prices), targetFilename)
	return nil
}

func WriteAppointmentsToCSV(appointments []medicalmodels.Appointment, targetFilename string) error {
	file, err := os.Create(targetFilename)
	if err != nil {
		return fmt.Errorf("failed to create csv file %s: %w", targetFilename, err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	headers := []string{"appointment_id", "patient_id", "doctor_id", "clinic_id", "appointment_date", "diagnosis"}
	if err := writer.Write(headers); err != nil {
		return fmt.Errorf("failed to write csv header to %s: %w", targetFilename, err)
	}

	record := make([]string, len(headers))
	for _, a := range appointments {
		record[0] = strconv.FormatInt(a.AppointmentID, 10)
		record[1] = strconv.Itoa(a.PatientID)
		record[2] = strconv.Itoa(a.DoctorID)
		record[3] = strconv.Itoa(a.ClinicID)
		record[4] = a.AppointmentDate.Format(time.RFC3339)
		record[5] = a.Diagnosis
		if err := writer.Write(record); err != nil {
			return fmt.Errorf("failed to write csv record to %s: %w", targetFilename, err)
		}
	}

	if err := writer.Error(); err != nil {
		return fmt.Errorf("error occurred during csv writing to %s: %w", targetFilename, err)
	}

	fmt.Printf("Successfully wrote %d records to %s\n", len(appointments), targetFilename)
	return nil
}

// --- High-Performance, Type-Specific CSV Writers for Medical ---

func WritePatientsToCSV(patients []medicalmodels.Patient, targetFilename string) error {
	file, err := os.Create(targetFilename)
	if err != nil {
		return fmt.Errorf("failed to create csv file %s: %w", targetFilename, err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	headers := []string{"patient_id", "patient_name", "date_of_birth", "gender"}
	if err := writer.Write(headers); err != nil {
		return fmt.Errorf("failed to write csv header to %s: %w", targetFilename, err)
	}

	record := make([]string, len(headers))
	for _, p := range patients {
		record[0] = strconv.Itoa(p.PatientID)
		record[1] = p.PatientName
		record[2] = p.DateOfBirth.Format(time.RFC3339)
		record[3] = p.Gender
		if err := writer.Write(record); err != nil {
			return fmt.Errorf("failed to write csv record to %s: %w", targetFilename, err)
		}
	}

	if err := writer.Error(); err != nil {
		return fmt.Errorf("error occurred during csv writing to %s: %w", targetFilename, err)
	}

	fmt.Printf("Successfully wrote %d records to %s\n", len(patients), targetFilename)
	return nil
}

func WriteDoctorsToCSV(doctors []medicalmodels.Doctor, targetFilename string) error {
	file, err := os.Create(targetFilename)
	if err != nil {
		return fmt.Errorf("failed to create csv file %s: %w", targetFilename, err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	headers := []string{"doctor_id", "doctor_name", "specialization"}
	if err := writer.Write(headers); err != nil {
		return fmt.Errorf("failed to write csv header to %s: %w", targetFilename, err)
	}

	record := make([]string, len(headers))
	for _, d := range doctors {
		record[0] = strconv.Itoa(d.DoctorID)
		record[1] = d.DoctorName
		record[2] = d.Specialization
		if err := writer.Write(record); err != nil {
			return fmt.Errorf("failed to write csv record to %s: %w", targetFilename, err)
		}
	}

	if err := writer.Error(); err != nil {
		return fmt.Errorf("error occurred during csv writing to %s: %w", targetFilename, err)
	}

	fmt.Printf("Successfully wrote %d records to %s\n", len(doctors), targetFilename)
	return nil
}

func WriteClinicsToCSV(clinics []medicalmodels.Clinic, targetFilename string) error {
	file, err := os.Create(targetFilename)
	if err != nil {
		return fmt.Errorf("failed to create csv file %s: %w", targetFilename, err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	headers := []string{"clinic_id", "clinic_name", "address"}
	if err := writer.Write(headers); err != nil {
		return fmt.Errorf("failed to write csv header to %s: %w", targetFilename, err)
	}

	record := make([]string, len(headers))
	for _, c := range clinics {
		record[0] = strconv.Itoa(c.ClinicID)
		record[1] = c.ClinicName
		record[2] = c.Address
		if err := writer.Write(record); err != nil {
			return fmt.Errorf("failed to write csv record to %s: %w", targetFilename, err)
		}
	}

	if err := writer.Error(); err != nil {
		return fmt.Errorf("error occurred during csv writing to %s: %w", targetFilename, err)
	}

	fmt.Printf("Successfully wrote %d records to %s\n", len(clinics), targetFilename)
	return nil
}

func WriteCustomerAddressesToCSV(addresses []ecommercemodels.CustomerAddress, targetFilename string) error {
	file, err := os.Create(targetFilename)
	if err != nil {
		return fmt.Errorf("failed to create csv file %s: %w", targetFilename, err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	headers := []string{"address_id", "customer_id", "address_type", "address", "city", "state", "zip", "country"}
	if err := writer.Write(headers); err != nil {
		return fmt.Errorf("failed to write csv header to %s: %w", targetFilename, err)
	}

	record := make([]string, len(headers))
	for _, a := range addresses {
		record[0] = strconv.Itoa(a.AddressID)
		record[1] = strconv.Itoa(a.CustomerID)
		record[2] = a.AddressType
		record[3] = a.Address
		record[4] = a.City
		record[5] = a.State
		record[6] = a.Zip
		record[7] = a.Country
		if err := writer.Write(record); err != nil {
			return fmt.Errorf("failed to write csv record to %s: %w", targetFilename, err)
		}
	}

	if err := writer.Error(); err != nil {
		return fmt.Errorf("error occurred during csv writing to %s: %w", targetFilename, err)
	}

	fmt.Printf("Successfully wrote %d records to %s\n", len(addresses), targetFilename)
	return nil
}

func WriteSuppliersToCSV(suppliers []ecommercemodels.Supplier, targetFilename string) error {
	file, err := os.Create(targetFilename)
	if err != nil {
		return fmt.Errorf("failed to create csv file %s: %w", targetFilename, err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	headers := []string{"supplier_id", "supplier_name", "country"}
	if err := writer.Write(headers); err != nil {
		return fmt.Errorf("failed to write csv header to %s: %w", targetFilename, err)
	}

	record := make([]string, len(headers))
	for _, s := range suppliers {
		record[0] = strconv.Itoa(s.SupplierID)
		record[1] = s.SupplierName
		record[2] = s.Country
		if err := writer.Write(record); err != nil {
			return fmt.Errorf("failed to write csv record to %s: %w", targetFilename, err)
		}
	}

	if err := writer.Error(); err != nil {
		return fmt.Errorf("error occurred during csv writing to %s: %w", targetFilename, err)
	}

	fmt.Printf("Successfully wrote %d records to %s\n", len(suppliers), targetFilename)
	return nil
}

func WriteProductCategoriesToCSV(categories []ecommercemodels.ProductCategory, targetFilename string) error {
	file, err := os.Create(targetFilename)
	if err != nil {
		return fmt.Errorf("failed to create csv file %s: %w", targetFilename, err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	headers := []string{"category_id", "category_name"}
	if err := writer.Write(headers); err != nil {
		return fmt.Errorf("failed to write csv header to %s: %w", targetFilename, err)
	}

	record := make([]string, len(headers))
	for _, c := range categories {
		record[0] = strconv.Itoa(c.CategoryID)
		record[1] = c.CategoryName
		if err := writer.Write(record); err != nil {
			return fmt.Errorf("failed to write csv record to %s: %w", targetFilename, err)
		}
	}

	if err := writer.Error(); err != nil {
		return fmt.Errorf("error occurred during csv writing to %s: %w", targetFilename, err)
	}

	fmt.Printf("Successfully wrote %d records to %s\n", len(categories), targetFilename)
	return nil
}

func WriteProductsToCSV(products []ecommercemodels.Product, targetFilename string) error {
	file, err := os.Create(targetFilename)
	if err != nil {
		return fmt.Errorf("failed to create csv file %s: %w", targetFilename, err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	headers := []string{"product_id", "supplier_id", "product_name", "category_id", "base_price"}
	if err := writer.Write(headers); err != nil {
		return fmt.Errorf("failed to write csv header to %s: %w", targetFilename, err)
	}

	record := make([]string, len(headers))
	for _, p := range products {
		record[0] = strconv.Itoa(p.ProductID)
		record[1] = strconv.Itoa(p.SupplierID)
		record[2] = p.ProductName
		record[3] = strconv.Itoa(p.CategoryID)
		record[4] = strconv.FormatFloat(p.BasePrice, 'f', 2, 64)
		if err := writer.Write(record); err != nil {
			return fmt.Errorf("failed to write csv record to %s: %w", targetFilename, err)
		}
	}

	if err := writer.Error(); err != nil {
		return fmt.Errorf("error occurred during csv writing to %s: %w", targetFilename, err)
	}

	fmt.Printf("Successfully wrote %d records to %s\n", len(products), targetFilename)
	return nil
}

// WriteStreamOrderHeadersToCSV writes OrderHeader structs from a channel to a CSV file.
func WriteStreamOrderHeadersToCSV(headerChan <-chan ecommercemodels.OrderHeader, targetFilename string) error {
	file, err := os.Create(targetFilename)
	if err != nil {
		return fmt.Errorf("failed to create csv file %s: %w", targetFilename, err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	headers := []string{"order_id", "customer_id", "shipping_address_id", "billing_address_id", "order_timestamp", "order_status"}
	if err := writer.Write(headers); err != nil {
		return fmt.Errorf("failed to write csv header to %s: %w", targetFilename, err)
	}

	record := make([]string, len(headers))
	var recordCount int64
	for h := range headerChan {
		record[0] = strconv.Itoa(h.OrderID)
		record[1] = strconv.Itoa(h.CustomerID)
		record[2] = strconv.Itoa(h.ShippingAddressID)
		record[3] = strconv.Itoa(h.BillingAddressID)
		record[4] = h.OrderTimestamp.Format(time.RFC3339)
		record[5] = h.OrderStatus
		if err := writer.Write(record); err != nil {
			return fmt.Errorf("failed to write csv record to %s: %w", targetFilename, err)
		}
		recordCount++
	}

	if err := writer.Error(); err != nil {
		return fmt.Errorf("error occurred during csv writing to %s: %w", targetFilename, err)
	}

	fmt.Printf("Successfully wrote %d records to %s\n", recordCount, targetFilename)
	return nil
}

// --- High-Performance, Type-Specific CSV Writers for Financial ---

func WriteCompaniesToCSV(companies []financialmodels.Company, targetFilename string) error {
	file, err := os.Create(targetFilename)
	if err != nil {
		return fmt.Errorf("failed to create csv file %s: %w", targetFilename, err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	headers := []string{"company_id", "company_name", "ticker_symbol", "sector"}
	if err := writer.Write(headers); err != nil {
		return fmt.Errorf("failed to write csv header to %s: %w", targetFilename, err)
	}

	record := make([]string, len(headers))
	for _, c := range companies {
		record[0] = strconv.Itoa(c.CompanyID)
		record[1] = c.CompanyName
		record[2] = c.TickerSymbol
		record[3] = c.Sector
		if err := writer.Write(record); err != nil {
			return fmt.Errorf("failed to write csv record to %s: %w", targetFilename, err)
		}
	}

	if err := writer.Error(); err != nil {
		return fmt.Errorf("error occurred during csv writing to %s: %w", targetFilename, err)
	}

	fmt.Printf("Successfully wrote %d records to %s\n", len(companies), targetFilename)
	return nil
}

func WriteExchangesToCSV(exchanges []financialmodels.Exchange, targetFilename string) error {
	file, err := os.Create(targetFilename)
	if err != nil {
		return fmt.Errorf("failed to create csv file %s: %w", targetFilename, err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	headers := []string{"exchange_id", "exchange_name", "country"}
	if err := writer.Write(headers); err != nil {
		return fmt.Errorf("failed to write csv header to %s: %w", targetFilename, err)
	}

	record := make([]string, len(headers))
	for _, e := range exchanges {
		record[0] = strconv.Itoa(e.ExchangeID)
		record[1] = e.ExchangeName
		record[2] = e.Country
		if err := writer.Write(record); err != nil {
			return fmt.Errorf("failed to write csv record to %s: %w", targetFilename, err)
		}
	}

	if err := writer.Error(); err != nil {
		return fmt.Errorf("error occurred during csv writing to %s: %w", targetFilename, err)
	}

	fmt.Printf("Successfully wrote %d records to %s\n", len(exchanges), targetFilename)
	return nil
}

// WriteStreamOrderItemsToCSV writes OrderItem structs from a channel to a CSV file.
func WriteStreamOrderItemsToCSV(itemChan <-chan ecommercemodels.OrderItem, targetFilename string) error {
	file, err := os.Create(targetFilename)
	if err != nil {
		return fmt.Errorf("failed to create csv file %s: %w", targetFilename, err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	headers := []string{"order_item_id", "order_id", "product_id", "quantity", "unit_price", "discount", "total_price"}
	if err := writer.Write(headers); err != nil {
		return fmt.Errorf("failed to write csv header to %s: %w", targetFilename, err)
	}

	record := make([]string, len(headers))
	var recordCount int64
	for item := range itemChan {
		record[0] = strconv.Itoa(item.OrderItemID)
		record[1] = strconv.Itoa(item.OrderID)
		record[2] = strconv.Itoa(item.ProductID)
		record[3] = strconv.Itoa(item.Quantity)
		record[4] = strconv.FormatFloat(item.UnitPrice, 'f', 2, 64)
		record[5] = strconv.FormatFloat(item.Discount, 'f', 4, 64)
		record[6] = strconv.FormatFloat(item.TotalPrice, 'f', 4, 64)
		if err := writer.Write(record); err != nil {
			return fmt.Errorf("failed to write csv record to %s: %w", targetFilename, err)
		}
		recordCount++
	}

	if err := writer.Error(); err != nil {
		return fmt.Errorf("error occurred during csv writing to %s: %w", targetFilename, err)
	}

	fmt.Printf("Successfully wrote %d records to %s\n", recordCount, targetFilename)
	return nil
}
