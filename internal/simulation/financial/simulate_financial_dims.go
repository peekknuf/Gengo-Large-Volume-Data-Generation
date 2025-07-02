package financial

import (
	"math/rand"

	gf "github.com/brianvoe/gofakeit/v6"
	"github.com/peekknuf/Gengo/internal/models/financial"
)

var (
	sectors = []string{"Technology", "Healthcare", "Financials", "Consumer Discretionary", "Communication Services", "Industrials", "Consumer Staples", "Energy", "Utilities", "Real Estate", "Materials"}
	exchangeNames = []string{"NASDAQ", "New York Stock Exchange", "London Stock Exchange", "Tokyo Stock Exchange", "Hong Kong Stock Exchange"}
)

func GenerateCompanies(count int) []financial.Company {
	if count <= 0 {
		return []financial.Company{}
	}
	companies := make([]financial.Company, count)
	for i := 0; i < count; i++ {
		companies[i] = financial.Company{
			CompanyID:    i + 1,
			CompanyName:  gf.Company(),
			TickerSymbol: gf.LetterN(4),
			Sector:       sectors[rand.Intn(len(sectors))],
		}
	}
	return companies
}

func GenerateExchanges(count int) []financial.Exchange {
	if count <= 0 {
		return []financial.Exchange{}
	}
	exchanges := make([]financial.Exchange, count)
	for i := 0; i < count; i++ {
		exchanges[i] = financial.Exchange{
			ExchangeID:   i + 1,
			ExchangeName: exchangeNames[i%len(exchangeNames)],
			Country:      gf.Country(),
		}
	}
	return exchanges
}
