// cmd/simulate_financial_dims.go
package cmd

import (
	"math/rand"

	gf "github.com/brianvoe/gofakeit/v6"
)

var (
	sectors = []string{"Technology", "Healthcare", "Financials", "Consumer Discretionary", "Communication Services", "Industrials", "Consumer Staples", "Energy", "Utilities", "Real Estate", "Materials"}
	exchangeNames = []string{"NASDAQ", "New York Stock Exchange", "London Stock Exchange", "Tokyo Stock Exchange", "Hong Kong Stock Exchange"}
)

func generateCompanies(count int) []Company {
	if count <= 0 {
		return []Company{}
	}
	companies := make([]Company, count)
	for i := 0; i < count; i++ {
		companies[i] = Company{
			CompanyID:    i + 1,
			CompanyName:  gf.Company(),
			TickerSymbol: gf.LetterN(4),
			Sector:       sectors[rand.Intn(len(sectors))],
		}
	}
	return companies
}

func generateExchanges(count int) []Exchange {
	if count <= 0 {
		return []Exchange{}
	}
	exchanges := make([]Exchange, count)
	for i := 0; i < count; i++ {
		exchanges[i] = Exchange{
			ExchangeID:   i + 1,
			ExchangeName: exchangeNames[i%len(exchangeNames)],
			Country:      gf.Country(),
		}
	}
	return exchanges
}
