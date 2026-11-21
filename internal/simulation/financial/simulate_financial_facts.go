package financial

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/peekknuf/Gengo/internal/formats"
	"github.com/peekknuf/Gengo/internal/generation"
	"github.com/peekknuf/Gengo/internal/models/financial"
)

// A company's RNG and previous close continue across every file boundary.
type priceSeries struct {
	rng       *rand.Rand
	lastClose float64
	date      time.Time
	companyID int
	exchanges []financial.Exchange
}

func newPriceSeries(companyID, count int, exchanges []financial.Exchange, cfg generation.Config) *priceSeries {
	rng := cfg.Rand("financial.prices", uint64(companyID))
	return &priceSeries{rng: rng, lastClose: 20 + rng.Float64()*480, date: cfg.AsOf.AddDate(0, 0, 1-count), companyID: companyID, exchanges: exchanges}
}
func (s *priceSeries) next(id int64) financial.DailyStockPrice {
	open := max(0.02, s.lastClose*(1+(s.rng.Float64()-0.5)*0.1))
	high := open * (1 + s.rng.Float64()*0.05)
	low := open * (1 - s.rng.Float64()*0.05)
	close := low + s.rng.Float64()*(high-low)
	row := financial.DailyStockPrice{PriceID: id, Date: s.date, CompanyID: s.companyID, ExchangeID: s.exchanges[s.rng.Intn(len(s.exchanges))].ExchangeID, OpenPrice: open, HighPrice: high, LowPrice: low, ClosePrice: close, Volume: 10000 + s.rng.Intn(9990001)}
	s.lastClose = close
	s.date = s.date.AddDate(0, 0, 1)
	return row
}

// Retained for the price invariant regression fixture.
func generateStockPricesForCompanies(companies []financial.Company, exchanges []financial.Exchange, numPricesPerCompany, extra int, priceIDStart int64, options ...generation.Config) []financial.DailyStockPrice {
	cfg := generation.Options(options)
	prices := make([]financial.DailyStockPrice, 0, len(companies)*numPricesPerCompany+extra)
	for i, c := range companies {
		count := numPricesPerCompany
		if i < extra {
			count++
		}
		series := newPriceSeries(c.CompanyID, count, exchanges, cfg)
		for range count {
			prices = append(prices, series.next(priceIDStart))
			priceIDStart++
		}
	}
	return prices
}
func generateAndWriteDailyStockPricesConcurrently(numPrices int, companies []financial.Company, exchanges []financial.Exchange, format, outputDir string, options ...generation.Config) error {
	cfg := generation.Options(options)
	if numPrices < 0 {
		return fmt.Errorf("stock price count must be nonnegative")
	}
	if numPrices == 0 {
		return nil
	}
	if len(companies) == 0 || len(exchanges) == 0 {
		return fmt.Errorf("stock prices require at least one company and exchange")
	}
	perCompany, extra := numPrices/len(companies), numPrices%len(companies)

	maxPerCompany := perCompany
	if extra > 0 {
		maxPerCompany++
	}
	earliestDay := time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC).Unix() / 86400
	availableDays := cfg.AsOf.Unix()/86400 - earliestDay + 1
	if int64(maxPerCompany) > availableDays {
		return fmt.Errorf("price history exceeds dates supported by the reference date")
	}
	companyJobs := cfg
	companyJobs.RowsPerFile = max(1, cfg.RowsPerFile/maxPerCompany)
	return generation.Partitions(context.Background(), len(companies), companyJobs, func(ctx context.Context, p generation.Partition) error {
		rows := make([]financial.DailyStockPrice, 0, min(cfg.RowsPerFile, numPrices))
		part := 0
		flush := func() error {
			if len(rows) == 0 {
				return nil
			}
			name := fmt.Sprintf("fact_daily_stock_prices_%d_%d", p.Index, part)
			if err := formats.WriteSliceData(rows, name, format, outputDir); err != nil {
				return err
			}
			if err := cfg.RecordFile(name+"."+format, len(rows), rows[0].PriceID, rows[len(rows)-1].PriceID); err != nil {
				return err
			}
			rows = rows[:0]
			part++
			return nil
		}
		for index := p.Start; index < p.Start+p.Count; index++ {
			count := perCompany
			if index < extra {
				count++
			}
			first := index*perCompany + min(index, extra) + 1
			series := newPriceSeries(companies[index].CompanyID, count, exchanges, cfg)
			for i := 0; i < count; i++ {
				if i%1024 == 0 && ctx.Err() != nil {
					return ctx.Err()
				}
				rows = append(rows, series.next(int64(first+i)))
				if len(rows) == cfg.RowsPerFile {
					if err := flush(); err != nil {
						return err
					}
				}
			}
		}
		return flush()
	})

}

type FinancialRowCounts struct{ Companies, Exchanges, DailyStockPrices int }

func GenerateFinancialModelData(counts FinancialRowCounts, companies []financial.Company, exchanges []financial.Exchange, format, outputDir string, options ...generation.Config) error {
	return generateAndWriteDailyStockPricesConcurrently(counts.DailyStockPrices, companies, exchanges, format, outputDir, options...)
}
