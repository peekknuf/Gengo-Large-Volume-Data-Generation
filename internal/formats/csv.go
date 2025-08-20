package formats

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"os"
	"strconv"
	"time"

	ecommercemodels "github.com/peekknuf/Gengo/internal/models/ecommerce"
	financialmodels "github.com/peekknuf/Gengo/internal/models/financial"
	medicalmodels "github.com/peekknuf/Gengo/internal/models/medical"
)

// writeCSVHeaderAndRecords is a helper to reduce boilerplate.
func writeCSVHeaderAndRecords(targetFilename string, headers []string, records [][]string) error {
	file, err := os.Create(targetFilename)
	if err != nil {
		return fmt.Errorf("failed to create csv file %s: %w", targetFilename, err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	if err := writer.Write(headers); err != nil {
		return fmt.Errorf("failed to write csv header to %s: %w", targetFilename, err)
	}
	if err := writer.WriteAll(records); err != nil {
		return fmt.Errorf("failed to write csv records to %s: %w", targetFilename, err)
	}
	if err := writer.Error(); err != nil {
		return fmt.Errorf("error occurred during csv writing to %s: %w", targetFilename, err)
	}
	fmt.Printf("Successfully wrote %d records to %s\n", len(records), targetFilename)
	return nil
}

// --- Dimension Writers (kept as original for focus) ---

func WriteCustomersToCSV(customers []ecommercemodels.Customer, targetFilename string) error {
	headers := []string{"customer_id", "first_name", "last_name", "email"}
	records := make([][]string, len(customers))
	for i, c := range customers {
		records[i] = []string{strconv.Itoa(c.CustomerID), c.FirstName, c.LastName, c.Email}
	}
	return writeCSVHeaderAndRecords(targetFilename, headers, records)
}

func WriteCustomerAddressesToCSV(addresses []ecommercemodels.CustomerAddress, targetFilename string) error {
	headers := []string{"address_id", "customer_id", "address_type", "address", "city", "state", "zip", "country"}
	records := make([][]string, len(addresses))
	for i, a := range addresses {
		records[i] = []string{strconv.Itoa(a.AddressID), strconv.Itoa(a.CustomerID), a.AddressType, a.Address, a.City, a.State, a.Zip, a.Country}
	}
	return writeCSVHeaderAndRecords(targetFilename, headers, records)
}

func WriteSuppliersToCSV(suppliers []ecommercemodels.Supplier, targetFilename string) error {
	headers := []string{"supplier_id", "supplier_name", "country"}
	records := make([][]string, len(suppliers))
	for i, s := range suppliers {
		records[i] = []string{strconv.Itoa(s.SupplierID), s.SupplierName, s.Country}
	}
	return writeCSVHeaderAndRecords(targetFilename, headers, records)
}

func WriteProductCategoriesToCSV(categories []ecommercemodels.ProductCategory, targetFilename string) error {
	headers := []string{"category_id", "category_name"}
	records := make([][]string, len(categories))
	for i, c := range categories {
		records[i] = []string{strconv.Itoa(c.CategoryID), c.CategoryName}
	}
	return writeCSVHeaderAndRecords(targetFilename, headers, records)
}

func WriteProductsToCSV(products []ecommercemodels.Product, targetFilename string) error {
	headers := []string{"product_id", "supplier_id", "product_name", "category_id", "base_price"}
	records := make([][]string, len(products))
	for i, p := range products {
		records[i] = []string{strconv.Itoa(p.ProductID), strconv.Itoa(p.SupplierID), p.ProductName, strconv.Itoa(p.CategoryID), strconv.FormatFloat(p.BasePrice, 'f', 2, 64)}
	}
	return writeCSVHeaderAndRecords(targetFilename, headers, records)
}

// --- Fact Table Streaming Writers ---

// WriteStreamOrderHeadersToCSV writes OrderHeader structs from a channel to a CSV file using a buffered writer.
func WriteStreamOrderHeadersToCSV(headerChan <-chan ecommercemodels.OrderHeader, targetFilename string) error {
	file, err := os.Create(targetFilename)
	if err != nil {
		return fmt.Errorf("failed to create csv file %s: %w", targetFilename, err)
	}
	defer file.Close()

	// Use a larger buffered writer for better performance with high-volume data
	bufferedWriter := bufio.NewWriterSize(file, 64*1024) // 64KB buffer
	defer bufferedWriter.Flush()

	writer := csv.NewWriter(bufferedWriter)
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
		
		// Periodically flush to avoid memory buildup
		if recordCount%10000 == 0 {
			writer.Flush()
			if err := writer.Error(); err != nil {
				return fmt.Errorf("error occurred during csv writing to %s: %w", targetFilename, err)
			}
		}
	}

	if err := writer.Error(); err != nil {
		return fmt.Errorf("error occurred during csv writing to %s: %w", targetFilename, err)
	}

	fmt.Printf("Successfully wrote %d records to %s\n", recordCount, targetFilename)
	return nil
}

// WriteStreamOrderItemsToCSV writes OrderItem structs from a channel to a CSV file using a buffered writer.
func WriteStreamOrderItemsToCSV(itemChan <-chan ecommercemodels.OrderItem, targetFilename string) error {
	file, err := os.Create(targetFilename)
	if err != nil {
		return fmt.Errorf("failed to create csv file %s: %w", targetFilename, err)
	}
	defer file.Close()

	// Use a larger buffered writer for better performance with high-volume data
	bufferedWriter := bufio.NewWriterSize(file, 64*1024) // 64KB buffer
	defer bufferedWriter.Flush()

	writer := csv.NewWriter(bufferedWriter)
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
		
		// Periodically flush to avoid memory buildup
		if recordCount%10000 == 0 {
			writer.Flush()
			if err := writer.Error(); err != nil {
				return fmt.Errorf("error occurred during csv writing to %s: %w", targetFilename, err)
			}
		}
	}

	if err := writer.Error(); err != nil {
		return fmt.Errorf("error occurred during csv writing to %s: %w", targetFilename, err)
	}

	fmt.Printf("Successfully wrote %d records to %s\n", recordCount, targetFilename)
	return nil
}

// --- Other Model Writers (kept as original for focus) ---

func WriteDailyStockPricesToCSV(prices []financialmodels.DailyStockPrice, targetFilename string) error {
	headers := []string{"price_id", "date", "company_id", "exchange_id", "open_price", "high_price", "low_price", "close_price", "volume"}
	records := make([][]string, len(prices))
	for i, p := range prices {
		records[i] = []string{
			strconv.FormatInt(p.PriceID, 10), p.Date.Format("2006-01-02"), strconv.Itoa(p.CompanyID),
			strconv.Itoa(p.ExchangeID), strconv.FormatFloat(p.OpenPrice, 'f', 4, 64),
			strconv.FormatFloat(p.HighPrice, 'f', 4, 64), strconv.FormatFloat(p.LowPrice, 'f', 4, 64),
			strconv.FormatFloat(p.ClosePrice, 'f', 4, 64), strconv.Itoa(p.Volume),
		}
	}
	return writeCSVHeaderAndRecords(targetFilename, headers, records)
}

func WriteAppointmentsToCSV(appointments []medicalmodels.Appointment, targetFilename string) error {
	headers := []string{"appointment_id", "patient_id", "doctor_id", "clinic_id", "appointment_date", "diagnosis"}
	records := make([][]string, len(appointments))
	for i, a := range appointments {
		records[i] = []string{
			strconv.FormatInt(a.AppointmentID, 10), strconv.Itoa(a.PatientID), strconv.Itoa(a.DoctorID),
			strconv.Itoa(a.ClinicID), a.AppointmentDate.Format(time.RFC3339), a.Diagnosis,
		}
	}
	return writeCSVHeaderAndRecords(targetFilename, headers, records)
}

func WritePatientsToCSV(patients []medicalmodels.Patient, targetFilename string) error {
	headers := []string{"patient_id", "patient_name", "date_of_birth", "gender"}
	records := make([][]string, len(patients))
	for i, p := range patients {
		records[i] = []string{strconv.Itoa(p.PatientID), p.PatientName, p.DateOfBirth.Format(time.RFC3339), p.Gender}
	}
	return writeCSVHeaderAndRecords(targetFilename, headers, records)
}

func WriteDoctorsToCSV(doctors []medicalmodels.Doctor, targetFilename string) error {
	headers := []string{"doctor_id", "doctor_name", "specialization"}
	records := make([][]string, len(doctors))
	for i, d := range doctors {
		records[i] = []string{strconv.Itoa(d.DoctorID), d.DoctorName, d.Specialization}
	}
	return writeCSVHeaderAndRecords(targetFilename, headers, records)
}

func WriteClinicsToCSV(clinics []medicalmodels.Clinic, targetFilename string) error {
	headers := []string{"clinic_id", "clinic_name", "address"}
	records := make([][]string, len(clinics))
	for i, c := range clinics {
		records[i] = []string{strconv.Itoa(c.ClinicID), c.ClinicName, c.Address}
	}
	return writeCSVHeaderAndRecords(targetFilename, headers, records)
}

func WriteCompaniesToCSV(companies []financialmodels.Company, targetFilename string) error {
	headers := []string{"company_id", "company_name", "ticker_symbol", "sector"}
	records := make([][]string, len(companies))
	for i, c := range companies {
		records[i] = []string{strconv.Itoa(c.CompanyID), c.CompanyName, c.TickerSymbol, c.Sector}
	}
	return writeCSVHeaderAndRecords(targetFilename, headers, records)
}