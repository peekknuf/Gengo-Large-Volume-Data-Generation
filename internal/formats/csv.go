package formats

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"time"
)

// WriteCSVFile shares buffered write and close error handling with direct encoders.
func WriteCSVFile(filename string, write func(*bufio.Writer) error) (err error) {
	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("create %s: %w", filename, err)
	}
	bw := bufio.NewWriterSize(file, 1024*1024)
	defer func() { err = errors.Join(err, bw.Flush(), file.Close()) }()
	return write(bw)
}

func appendCSVRecord(buf []byte, fields []string) []byte {
	for i, field := range fields {
		if i > 0 {
			buf = append(buf, ',')
		}
		buf = appendCSVField(buf, field)
	}
	return append(buf, '\n')
}

// appendCSVRow formats into reusable storage without per-field strings.
func appendCSVRow(buf []byte, row reflect.Value) []byte {
	for row.Kind() == reflect.Interface || row.Kind() == reflect.Ptr {
		row = row.Elem()
	}
	for i := 0; i < row.NumField(); i++ {
		if i > 0 {
			buf = append(buf, ',')
		}
		field := row.Field(i)
		switch field.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			buf = strconv.AppendInt(buf, field.Int(), 10)
		case reflect.Float32, reflect.Float64:
			buf = strconv.AppendFloat(buf, field.Float(), 'f', -1, field.Type().Bits())
		case reflect.String:
			buf = appendCSVField(buf, field.String())
		case reflect.Struct:
			if t, ok := field.Interface().(time.Time); ok {
				buf = t.AppendFormat(buf, time.RFC3339Nano)
			}
		}
	}
	return append(buf, '\n')
}

// WriteSliceData writes a slice of structs in the selected format.
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
		return nil
	}
	start := time.Now()
	err := WriteCSVFile(targetFilename, func(bw *bufio.Writer) error {
		rowBuf := appendCSVRecord(make([]byte, 0, 1024), getCSVHeaders(slice.Index(0).Interface()))
		if _, err := bw.Write(rowBuf); err != nil {
			return err
		}
		for i := 0; i < slice.Len(); i++ {
			rowBuf = appendCSVRow(rowBuf[:0], slice.Index(i))
			if _, err := bw.Write(rowBuf); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	fmt.Printf("Successfully wrote %d records to %s in %s\n", slice.Len(), targetFilename, time.Since(start).Round(time.Millisecond))
	return nil
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
