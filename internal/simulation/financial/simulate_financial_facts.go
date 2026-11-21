package financial

import (
	"fmt"
	"math/rand"
	"runtime"
	"sync"
	"time"

	gf "github.com/brianvoe/gofakeit/v6"
	"github.com/peekknuf/Gengo/internal/formats"
	"github.com/peekknuf/Gengo/internal/models/financial"
)

const NumYearsOfData = 5

// generateStockPricesForCompanies is a worker function that generates stock prices for a subset of companies.
func generateStockPricesForCompanies(companies []financial.Company, exchanges []financial.Exchange, numPricesPerCompany, extra int, priceIDStart int64) []financial.DailyStockPrice {
	prices := make([]financial.DailyStockPrice, 0, len(companies)*numPricesPerCompany+extra)
	priceIDCounter := priceIDStart

	for index, company := range companies {
		count := numPricesPerCompany
		if index < extra {
			count++
		}
		lastClose := gf.Float64Range(20, 500)
		date := time.Now().AddDate(-NumYearsOfData, 0, 0)

		for i := 0; i < count; i++ {
			openPrice := max(0.02, lastClose*(1+(rand.Float64()-0.5)*0.1))
			highPrice := openPrice * (1 + rand.Float64()*0.05)
			lowPrice := openPrice * (1 - rand.Float64()*0.05)
			closePrice := lowPrice + rand.Float64()*(highPrice-lowPrice)

			prices = append(prices, financial.DailyStockPrice{
				PriceID:    priceIDCounter,
				Date:       date,
				CompanyID:  company.CompanyID,
				ExchangeID: exchanges[rand.Intn(len(exchanges))].ExchangeID,
				OpenPrice:  openPrice,
				HighPrice:  highPrice,
				LowPrice:   lowPrice,
				ClosePrice: closePrice,
				Volume:     gf.Number(10000, 10000000),
			})

			lastClose = closePrice
			date = date.AddDate(0, 0, 1)
			priceIDCounter++
		}
	}
	return prices
}

func generateAndWriteDailyStockPricesConcurrently(numPrices int, companies []financial.Company, exchanges []financial.Exchange, format string, outputDir string) error {
	if numPrices < 0 {
		return fmt.Errorf("stock price count must be nonnegative")
	}
	if numPrices > 0 && (len(companies) == 0 || len(exchanges) == 0) {
		return fmt.Errorf("stock prices require at least one company and exchange")
	}
	if numPrices == 0 {
		return nil
	}

	numWorkers := min(runtime.GOMAXPROCS(0), len(companies), numPrices)
	var wg sync.WaitGroup
	resultsChan := make(chan []financial.DailyStockPrice, numWorkers)

	avgPricesPerCompany := numPrices / len(companies)
	remainder := numPrices % len(companies)
	chunkSize := 1 + (len(companies)-1)/numWorkers
	for start := 0; start < len(companies); start += chunkSize {
		end := min(start+chunkSize, len(companies))
		extra := max(0, min(end, remainder)-start)
		if avgPricesPerCompany == 0 && extra == 0 {
			break
		}
		startID := int64(start*avgPricesPerCompany+min(start, remainder)) + 1
		wg.Add(1)
		go func() {
			defer wg.Done()
			resultsChan <- generateStockPricesForCompanies(companies[start:end], exchanges, avgPricesPerCompany, extra, startID)
		}()
	}
	wg.Wait()
	close(resultsChan)

	fmt.Println("Aggregating results...")
	allPrices := make([]financial.DailyStockPrice, 0, numPrices)
	for prices := range resultsChan {
		allPrices = append(allPrices, prices...)
	}

	fmt.Println("Writing fact data to file...")
	return formats.WriteSliceData(allPrices, "fact_daily_stock_prices", format, outputDir)
}

type FinancialRowCounts struct {
	Companies        int
	Exchanges        int
	DailyStockPrices int
}

func GenerateFinancialModelData(counts FinancialRowCounts, companies []financial.Company, exchanges []financial.Exchange, format string, outputDir string) error {
	// Generate and write daily stock prices concurrently
	if err := generateAndWriteDailyStockPricesConcurrently(counts.DailyStockPrices, companies, exchanges, format, outputDir); err != nil {
		return fmt.Errorf("error generating daily stock prices: %w", err)
	}

	return nil
}
