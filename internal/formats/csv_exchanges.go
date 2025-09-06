package formats

import (
	"strconv"

	financialmodels "github.com/peekknuf/Gengo/internal/models/financial"
)

func WriteExchangesToCSV(exchanges []financialmodels.Exchange, targetFilename string) error {
	headers := []string{"exchange_id", "exchange_name", "country"}
	records := make([][]string, len(exchanges))
	for i, e := range exchanges {
		records[i] = []string{strconv.Itoa(e.ExchangeID), e.ExchangeName, e.Country}
	}
	return writeCSVHeaderAndRecords(targetFilename, headers, records)
}