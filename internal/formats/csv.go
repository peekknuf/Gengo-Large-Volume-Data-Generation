package formats

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"time"

	ecommercemodels "github.com/peekknuf/Gengo/internal/models/ecommerce"
	financialmodels "github.com/peekknuf/Gengo/internal/models/financial"
	medicalmodels "github.com/peekknuf/Gengo/internal/models/medical"
)

var digitsLUT = [100]string{
	"00", "01", "02", "03", "04", "05", "06", "07", "08", "09",
	"10", "11", "12", "13", "14", "15", "16", "17", "18", "19",
	"20", "21", "22", "23", "24", "25", "26", "27", "28", "29",
	"30", "31", "32", "33", "34", "35", "36", "37", "38", "39",
	"40", "41", "42", "43", "44", "45", "46", "47", "48", "49",
	"50", "51", "52", "53", "54", "55", "56", "57", "58", "59",
	"60", "61", "62", "63", "64", "65", "66", "67", "68", "69",
	"70", "71", "72", "73", "74", "75", "76", "77", "78", "79",
	"80", "81", "82", "83", "84", "85", "86", "87", "88", "89",
	"90", "91", "92", "93", "94", "95", "96", "97", "98", "99",
}

func fastItoa(buf []byte, v int64) []byte {
	if v < 10 {
		return append(buf, byte('0'+v))
	}
	var tmp [20]byte
	pos := len(tmp)
	for v >= 100 {
		pos -= 2
		copy(tmp[pos:], digitsLUT[v%100])
		v /= 100
	}
	if v < 10 {
		pos--
		tmp[pos] = byte('0' + v)
	} else {
		pos -= 2
		copy(tmp[pos:], digitsLUT[v])
	}
	return append(buf, tmp[pos:]...)
}

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
		return WriteSliceToParquet(data, filepath.Join(outputDir, filename+".parquet"))
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
		tag := t.Field(i).Tag.Get("csv")
		if tag == "" {
			tag = t.Field(i).Tag.Get("json")
		}
		if tag == "" {
			tag = t.Field(i).Tag.Get("parquet")
		}
		if tag != "" {
			tag = strings.SplitN(tag, ",", 2)[0]
		}
		headers[i] = tag
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
	bufferedWriter := bufio.NewWriterSize(file, 16*1024*1024) // 16MB buffer
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
	for i := 0; i < len(field); i++ {
		c := field[i]
		if c == ',' || c == '"' || c == '\n' || c == '\r' {
			return true
		}
	}
	return false
}

func appendCSVField(buf []byte, field string) []byte {
	if needsQuoting(field) {
		buf = append(buf, '"')
		for i := 0; i < len(field); i++ {
			if field[i] == '"' {
				buf = append(buf, '"', '"')
			} else {
				buf = append(buf, field[i])
			}
		}
		buf = append(buf, '"')
	} else {
		buf = append(buf, field...)
	}
	return buf
}

// --- Dimension Writers (kept as original for focus) ---

func WriteCustomersToCSV(customers []ecommercemodels.Customer, targetFilename string) error {
	startTime := time.Now()
	file, err := os.Create(targetFilename)
	if err != nil {
		return fmt.Errorf("failed to create csv file %s: %w", targetFilename, err)
	}
	defer file.Close()

	bw := bufio.NewWriterSize(file, 16*1024*1024)
	defer bw.Flush()

	bw.WriteString("customer_id,first_name,last_name,email\n")

	rowBuf := make([]byte, 0, 256)
	for _, c := range customers {
		rowBuf = rowBuf[:0]
		rowBuf = fastItoa(rowBuf, int64(c.CustomerID))
		rowBuf = append(rowBuf, ',')
		rowBuf = appendCSVField(rowBuf, c.FirstName)
		rowBuf = append(rowBuf, ',')
		rowBuf = appendCSVField(rowBuf, c.LastName)
		rowBuf = append(rowBuf, ',')
		rowBuf = appendCSVField(rowBuf, c.Email)
		rowBuf = append(rowBuf, '\n')
		bw.Write(rowBuf)
	}

	duration := time.Since(startTime)
	fmt.Printf("Successfully wrote %d records to %s in %s\n", len(customers), targetFilename, duration.Round(time.Millisecond))
	return nil
}

func WriteCustomerAddressesToCSV(addresses []ecommercemodels.CustomerAddress, targetFilename string) error {
	startTime := time.Now()
	file, err := os.Create(targetFilename)
	if err != nil {
		return fmt.Errorf("failed to create csv file %s: %w", targetFilename, err)
	}
	defer file.Close()

	bw := bufio.NewWriterSize(file, 16*1024*1024)
	defer bw.Flush()

	bw.WriteString("address_id,customer_id,address_type,address,city,state,zip,country\n")

	rowBuf := make([]byte, 0, 512)
	for _, a := range addresses {
		rowBuf = rowBuf[:0]
		rowBuf = fastItoa(rowBuf, int64(a.AddressID))
		rowBuf = append(rowBuf, ',')
		rowBuf = fastItoa(rowBuf, int64(a.CustomerID))
		rowBuf = append(rowBuf, ',')
		rowBuf = append(rowBuf, a.AddressType...)
		rowBuf = append(rowBuf, ',')
		rowBuf = appendCSVField(rowBuf, a.Address)
		rowBuf = append(rowBuf, ',')
		rowBuf = append(rowBuf, a.City...)
		rowBuf = append(rowBuf, ',')
		rowBuf = append(rowBuf, a.State...)
		rowBuf = append(rowBuf, ',')
		rowBuf = append(rowBuf, a.Zip...)
		rowBuf = append(rowBuf, ',')
		rowBuf = append(rowBuf, a.Country...)
		rowBuf = append(rowBuf, '\n')
		bw.Write(rowBuf)
	}

	duration := time.Since(startTime)
	fmt.Printf("Successfully wrote %d records to %s in %s\n", len(addresses), targetFilename, duration.Round(time.Millisecond))
	return nil
}

func WriteSuppliersToCSV(suppliers []ecommercemodels.Supplier, targetFilename string) error {
	startTime := time.Now()
	file, err := os.Create(targetFilename)
	if err != nil {
		return fmt.Errorf("failed to create csv file %s: %w", targetFilename, err)
	}
	defer file.Close()

	bw := bufio.NewWriterSize(file, 16*1024*1024)
	defer bw.Flush()

	bw.WriteString("supplier_id,supplier_name,country\n")

	rowBuf := make([]byte, 0, 256)
	for _, s := range suppliers {
		rowBuf = rowBuf[:0]
		rowBuf = fastItoa(rowBuf, int64(s.SupplierID))
		rowBuf = append(rowBuf, ',')
		rowBuf = appendCSVField(rowBuf, s.SupplierName)
		rowBuf = append(rowBuf, ',')
		rowBuf = append(rowBuf, s.Country...)
		rowBuf = append(rowBuf, '\n')
		bw.Write(rowBuf)
	}

	duration := time.Since(startTime)
	fmt.Printf("Successfully wrote %d records to %s in %s\n", len(suppliers), targetFilename, duration.Round(time.Millisecond))
	return nil
}

func WriteProductCategoriesToCSV(categories []ecommercemodels.ProductCategory, targetFilename string) error {
	startTime := time.Now()
	file, err := os.Create(targetFilename)
	if err != nil {
		return fmt.Errorf("failed to create csv file %s: %w", targetFilename, err)
	}
	defer file.Close()

	bw := bufio.NewWriterSize(file, 16*1024*1024)
	defer bw.Flush()

	bw.WriteString("category_id,category_name\n")

	rowBuf := make([]byte, 0, 128)
	for _, c := range categories {
		rowBuf = rowBuf[:0]
		rowBuf = fastItoa(rowBuf, int64(c.CategoryID))
		rowBuf = append(rowBuf, ',')
		rowBuf = append(rowBuf, c.CategoryName...)
		rowBuf = append(rowBuf, '\n')
		bw.Write(rowBuf)
	}

	duration := time.Since(startTime)
	fmt.Printf("Successfully wrote %d records to %s in %s\n", len(categories), targetFilename, duration.Round(time.Millisecond))
	return nil
}

func WriteProductsToCSV(products []ecommercemodels.Product, targetFilename string) error {
	startTime := time.Now()
	file, err := os.Create(targetFilename)
	if err != nil {
		return fmt.Errorf("failed to create csv file %s: %w", targetFilename, err)
	}
	defer file.Close()

	bw := bufio.NewWriterSize(file, 16*1024*1024)
	defer bw.Flush()

	bw.WriteString("product_id,supplier_id,product_name,category_id,base_price\n")

	rowBuf := make([]byte, 0, 256)
	for _, p := range products {
		rowBuf = rowBuf[:0]
		rowBuf = fastItoa(rowBuf, int64(p.ProductID))
		rowBuf = append(rowBuf, ',')
		rowBuf = fastItoa(rowBuf, int64(p.SupplierID))
		rowBuf = append(rowBuf, ',')
		rowBuf = appendCSVField(rowBuf, p.ProductName)
		rowBuf = append(rowBuf, ',')
		rowBuf = fastItoa(rowBuf, int64(p.CategoryID))
		rowBuf = append(rowBuf, ',')
		rowBuf = strconv.AppendFloat(rowBuf, p.BasePrice, 'f', 2, 64)
		rowBuf = append(rowBuf, '\n')
		bw.Write(rowBuf)
	}

	duration := time.Since(startTime)
	fmt.Printf("Successfully wrote %d records to %s in %s\n", len(products), targetFilename, duration.Round(time.Millisecond))
	return nil
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
		rowBuf = fastItoa(rowBuf, int64(h.OrderID))
		rowBuf = append(rowBuf, ',')
		rowBuf = fastItoa(rowBuf, int64(h.CustomerID))
		rowBuf = append(rowBuf, ',')
		rowBuf = fastItoa(rowBuf, int64(h.ShippingAddressID))
		rowBuf = append(rowBuf, ',')
		rowBuf = fastItoa(rowBuf, int64(h.BillingAddressID))
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
		rowBuf = fastItoa(rowBuf, int64(item.OrderItemID))
		rowBuf = append(rowBuf, ',')
		rowBuf = fastItoa(rowBuf, int64(item.OrderID))
		rowBuf = append(rowBuf, ',')
		rowBuf = fastItoa(rowBuf, int64(item.ProductID))
		rowBuf = append(rowBuf, ',')
		rowBuf = fastItoa(rowBuf, int64(item.Quantity))
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
