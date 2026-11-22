package financial

import (
	"github.com/peekknuf/Gengo/internal/generation"
	"github.com/peekknuf/Gengo/internal/models/financial"
)

var (
	sectors       = []string{"Technology", "Healthcare", "Financials", "Consumer Discretionary", "Communication Services", "Industrials", "Consumer Staples", "Energy", "Utilities", "Real Estate", "Materials"}
	exchangeNames = []string{"NASDAQ", "New York Stock Exchange", "London Stock Exchange", "Tokyo Stock Exchange", "Hong Kong Stock Exchange"}
)

func GenerateCompanies(count int, cfg generation.Config) []financial.Company {
	rng := cfg.Rand("financial.GenerateCompanies", 0)
	fake := cfg.Faker("financial.GenerateCompanies")
	if count <= 0 {
		return []financial.Company{}
	}
	companies := make([]financial.Company, count)
	for i := 0; i < count; i++ {
		companies[i] = financial.Company{
			CompanyID:    i + 1,
			CompanyName:  fake.Company(),
			TickerSymbol: fake.LetterN(4),
			Sector:       sectors[rng.Intn(len(sectors))],
		}
	}
	return companies
}

func GenerateExchanges(count int, cfg generation.Config) []financial.Exchange {
	fake := cfg.Faker("financial.GenerateExchanges")
	if count <= 0 {
		return []financial.Exchange{}
	}
	exchanges := make([]financial.Exchange, count)
	for i := 0; i < count; i++ {
		exchanges[i] = financial.Exchange{
			ExchangeID:   i + 1,
			ExchangeName: exchangeNames[i%len(exchangeNames)],
			Country:      fake.Country(),
		}
	}
	return exchanges
}
