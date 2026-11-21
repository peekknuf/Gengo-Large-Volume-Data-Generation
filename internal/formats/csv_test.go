package formats

import (
	"bufio"
	"encoding/csv"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

type failingCSVWriter struct{ err error }

func (w failingCSVWriter) Write(p []byte) (int, error) { return 0, w.err }

func TestCSVReportsFlushFailure(t *testing.T) {
	want := errors.New("simulated disk failure")
	err := writeCSVFile(filepath.Join(t.TempDir(), "rows.csv"), func(bw *bufio.Writer) error {
		bw.Reset(failingCSVWriter{want})
		_, err := bw.WriteString("buffered data")
		return err
	})
	if !errors.Is(err, want) {
		t.Fatalf("got %v, want flush failure", err)
	}
}

type csvTestRow struct {
	ID    int64     `json:"id"`
	Name  string    `json:"name"`
	Price float64   `json:"price"`
	Date  time.Time `json:"date"`
}

func TestCSVRoundTrip(t *testing.T) {
	rows := []csvTestRow{{-12, "Málaga, \"quoted\"\nsecond line", 12.5, time.Date(2025, 1, 2, 3, 4, 5, 0, time.UTC)}}
	want := [][]string{{"id", "name", "price", "date"}, {"-12", rows[0].Name, "12.5", "2025-01-02T03:04:05Z"}}
	for _, mode := range []string{"slice", "stream", "records"} {
		t.Run(mode, func(t *testing.T) {
			filename := filepath.Join(t.TempDir(), "rows.csv")
			var err error
			switch mode {
			case "slice":
				err = writeSliceToCSV(rows, filename)
			case "stream":
				ch := make(chan interface{}, 1)
				ch <- rows[0]
				close(ch)
				err = writeStreamToCSV(ch, filename)
			case "records":
				err = writeCSVHeaderAndRecords(filename, want[0], want[1:])
			}
			if err != nil {
				t.Fatal(err)
			}
			f, err := os.Open(filename)
			if err != nil {
				t.Fatal(err)
			}
			defer f.Close()
			got, err := csv.NewReader(f).ReadAll()
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("got %#v, want %#v", got, want)
			}
		})
	}
}

func BenchmarkWriteSliceCSV(b *testing.B) {
	rows := make([]csvTestRow, 100000)
	for i := range rows {
		rows[i] = csvTestRow{int64(i + 1), "Example customer", 123.45, time.Date(2025, 1, 2, 0, 0, 0, 0, time.UTC)}
	}
	filename := filepath.Join(b.TempDir(), "rows.csv")
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if err := writeSliceToCSV(rows, filename); err != nil {
			b.Fatal(err)
		}
	}
}
