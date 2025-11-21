package formats

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"time"

	ecommercemodels "github.com/peekknuf/Gengo/internal/models/ecommerce"
	financialmodels "github.com/peekknuf/Gengo/internal/models/financial"
	medicalmodels "github.com/peekknuf/Gengo/internal/models/medical"
)

// WriteStream writes data from a channel to a CSV file.
func WriteStream(data <-chan interface{}, filename, format, outputDir string) error {
	switch format {
	case "csv":
		return writeStreamToCSV(data, filepath.Join(outputDir, filename+".csv"))
	// Add other formats here if needed
	default:
		return fmt.Errorf("unsupported format: %s", format)
	}
}

func writeStreamToCSV(data <-chan interface{}, targetFilename string) error {
	file, err := os.Create(targetFilename)
	if err != nil {
		return fmt.Errorf("failed to create csv file %s: %w", targetFilename, err)
	}
	defer file.Close()

	bufferedWriter := bufio.NewWriterSize(file, 1024*1024) // 1MB buffer
	defer bufferedWriter.Flush()

	first := true
	for v := range data {
		if first {
			headers := getCSVHeaders(v)
			for i, header := range headers {
				if i > 0 {
					bufferedWriter.WriteByte(',')
				}
				bufferedWriter.WriteString(header)
			}
			bufferedWriter.WriteByte('\n')
			first = false
		}

		record := toCSVRecord(v)
		for i, field := range record {
			if i > 0 {
				bufferedWriter.WriteByte(',')
			}
			if needsQuoting(field) {
				bufferedWriter.WriteByte('"')
				for _, r := range field {
					if r == '"' {
						bufferedWriter.WriteString("\"")
					} else {
						bufferedWriter.WriteRune(r)
					}
				}
				bufferedWriter.WriteByte('"')
			} else {
				bufferedWriter.WriteString(field)
			}
		}
		bufferedWriter.WriteByte('\n')
	}

	return nil
}

// WriteSliceData writes any slice of structs to a CSV file.
func WriteSliceData(data interface{}, filename, format, outputDir string) error {
	switch format {
	case "csv":
		return writeSliceToCSV(data, filepath.Join(outputDir, filename+".csv"))
	case "parquet":
		return writeSliceToParquet(data, filepath.Join(outputDir, filename+".parquet"))
	default:
		return fmt.Errorf("unsupported format: %s", format)
	}
}

func writeSliceToCSV(data interface{}, targetFilename string) error {
	slice := reflect.ValueOf(data)
	if slice.Kind() != reflect.Slice {
		return fmt.Errorf("data is not a slice")
	}

	if slice.Len() == 0 {
		return nil // Nothing to write
	}

	headers := getCSVHeaders(slice.Index(0).Interface())
	records := make([][]string, slice.Len())

	for i := 0; i < slice.Len(); i++ {
		records[i] = toCSVRecord(slice.Index(i).Interface())
	}

	return writeCSVHeaderAndRecords(targetFilename, headers, records)
}

func getCSVHeaders(v interface{}) []string {
	t := reflect.TypeOf(v)
	headers := make([]string, t.NumField())
	for i := 0; i < t.NumField(); i++ {
		headers[i] = t.Field(i).Tag.Get("csv")
	}
	return headers
}

func toCSVRecord(v interface{}) []string {
	val := reflect.ValueOf(v)
	record := make([]string, val.NumField())
	for i := 0; i < val.NumField(); i++ {
		field := val.Field(i)
		switch field.Kind() {
		case reflect.Int, reflect.Int64:
			record[i] = strconv.FormatInt(field.Int(), 10)
		case reflect.Float64:
			record[i] = strconv.FormatFloat(field.Float(), 'f', -1, 64)
		case reflect.String:
			record[i] = field.String()
		case reflect.Struct:
			if t, ok := field.Interface().(time.Time); ok {
				record[i] = t.Format(time.RFC3339)
			}
		}
	}
	return record
}

// writeCSVHeaderAndRecords writes CSV data using direct byte formatting instead of encoding/csv
// for better performance with large datasets
func writeCSVHeaderAndRecords(targetFilename string, headers []string, records [][]string) error {
	startTime := time.Now()
	file, err := os.Create(targetFilename)
	if err != nil {
		return fmt.Errorf("failed to create csv file %s: %w", targetFilename, err)
	}
	defer file.Close()

	// Use large buffer for better I/O performance
	bufferedWriter := bufio.NewWriterSize(file, 1024*1024) // 1MB buffer
	defer bufferedWriter.Flush()

	// Write header directly with byte operations
	for i, header := range headers {
		if i > 0 {
			bufferedWriter.WriteByte(',')
		}
		bufferedWriter.WriteString(header)
	}
	bufferedWriter.WriteByte('\n')

	// Write records directly with byte operations
	for _, record := range records {
		for i, field := range record {
			if i > 0 {
				bufferedWriter.WriteByte(',')
			}
			// Simple field writing - escape if contains comma or quote
			if needsQuoting(field) {
				bufferedWriter.WriteByte('"')
				for _, r := range field {
					if r == '"' {
						bufferedWriter.WriteString("\"")
					} else {
						bufferedWriter.WriteRune(r)
					}
				}
				bufferedWriter.WriteByte('"')
			} else {
				bufferedWriter.WriteString(field)
			}
		}
		bufferedWriter.WriteByte('\n')
	}

	duration := time.Since(startTime)
	fmt.Printf("Successfully wrote %d records to %s in %s\n", len(records), targetFilename, duration.Round(time.Millisecond))
	return nil
}

// needsQuoting checks if a field needs CSV quoting
func needsQuoting(field string) bool {
	for _, r := range field {
		if r == ',' || r == '"' || r == '\n' || r == '\r' {
			return true
		}
	}
	return false
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
// WriteStreamOrderHeadersToCSV writes order headers using direct byte formatting for better performance
func WriteStreamOrderHeadersToCSV(headerChan <-chan ecommercemodels.OrderHeader, targetFilename string) error {
	file, err := os.Create(targetFilename)
	if err != nil {
		return fmt.Errorf("failed to create csv file %s: %w", targetFilename, err)
	}
	defer file.Close()

	// Use large buffered writer for better performance with high-volume data
	bufferedWriter := bufio.NewWriterSize(file, 2*1024*1024) // 2MB buffer
	defer bufferedWriter.Flush()

	// Write header directly
	bufferedWriter.WriteString("order_id,customer_id,shipping_address_id,billing_address_id,order_timestamp,order_status\n")

	// Pre-allocate buffer for row construction to reduce allocations
	rowBuf := make([]byte, 0, 1024) // 1KB buffer per row for better performance
	var recordCount int64

	for h := range headerChan {
		// Reset buffer for reuse
		rowBuf = rowBuf[:0]

		// Build row with direct byte operations
		rowBuf = strconv.AppendInt(rowBuf, int64(h.OrderID), 10)
		rowBuf = append(rowBuf, ',')
		rowBuf = strconv.AppendInt(rowBuf, int64(h.CustomerID), 10)
		rowBuf = append(rowBuf, ',')
		rowBuf = strconv.AppendInt(rowBuf, int64(h.ShippingAddressID), 10)
		rowBuf = append(rowBuf, ',')
		rowBuf = strconv.AppendInt(rowBuf, int64(h.BillingAddressID), 10)
		rowBuf = append(rowBuf, ',')

		// Format timestamp
		timestamp := h.OrderTimestamp.Format(time.RFC3339)
		rowBuf = append(rowBuf, timestamp...)
		rowBuf = append(rowBuf, ',')

		// Add order status (escape if needed)
		if needsQuoting(h.OrderStatus) {
			rowBuf = append(rowBuf, '"')
			for _, r := range h.OrderStatus {
				if r == '"' {
					rowBuf = append(rowBuf, '"', '"')
				} else {
					rowBuf = append(rowBuf, byte(r))
				}
			}
			rowBuf = append(rowBuf, '"')
		} else {
			rowBuf = append(rowBuf, h.OrderStatus...)
		}
		rowBuf = append(rowBuf, '\n')

		// Write the row
		if _, err := bufferedWriter.Write(rowBuf); err != nil {
			return fmt.Errorf("failed to write csv record to %s: %w", targetFilename, err)
		}
		recordCount++

		// Periodically flush to avoid memory buildup
		if recordCount%20000 == 0 {
			bufferedWriter.Flush()
		}
	}

	fmt.Printf("Successfully wrote %d order header records to %s\n", recordCount, targetFilename)
	return nil
}

// WriteStreamOrderItemsToCSV writes OrderItem structs from a channel to a CSV file using a buffered writer.
// WriteStreamOrderItemsToCSV writes order items using direct byte formatting for better performance
func WriteStreamOrderItemsToCSV(itemChan <-chan ecommercemodels.OrderItem, targetFilename string) error {
	file, err := os.Create(targetFilename)
	if err != nil {
		return fmt.Errorf("failed to create csv file %s: %w", targetFilename, err)
	}
	defer file.Close()

	// Use large buffered writer for better performance with high-volume data
	bufferedWriter := bufio.NewWriterSize(file, 2*1024*1024) // 2MB buffer
	defer bufferedWriter.Flush()

	// Write header directly
	bufferedWriter.WriteString("order_item_id,order_id,product_id,quantity,unit_price,discount\n")

	// Pre-allocate buffer for row construction to reduce allocations
	rowBuf := make([]byte, 0, 1024) // 1KB buffer per row for better performance
	var recordCount int64

	for item := range itemChan {
		// Reset buffer for reuse
		rowBuf = rowBuf[:0]

		// Build row with direct byte operations
		rowBuf = strconv.AppendInt(rowBuf, int64(item.OrderItemID), 10)
		rowBuf = append(rowBuf, ',')
		rowBuf = strconv.AppendInt(rowBuf, int64(item.OrderID), 10)
		rowBuf = append(rowBuf, ',')
		rowBuf = strconv.AppendInt(rowBuf, int64(item.ProductID), 10)
		rowBuf = append(rowBuf, ',')
		rowBuf = strconv.AppendInt(rowBuf, int64(item.Quantity), 10)
		rowBuf = append(rowBuf, ',')
		rowBuf = strconv.AppendFloat(rowBuf, item.UnitPrice, 'f', 2, 64)
		rowBuf = append(rowBuf, ',')
		rowBuf = strconv.AppendFloat(rowBuf, item.Discount, 'f', 4, 64)
		rowBuf = append(rowBuf, '\n')

		// Write the row
		if _, err := bufferedWriter.Write(rowBuf); err != nil {
			return fmt.Errorf("failed to write csv record to %s: %w", targetFilename, err)
		}
		recordCount++

		// Periodically flush to avoid memory buildup
		if recordCount%20000 == 0 {
			bufferedWriter.Flush()
		}
	}

	fmt.Printf("Successfully wrote %d order item records to %s\n", recordCount, targetFilename)
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
