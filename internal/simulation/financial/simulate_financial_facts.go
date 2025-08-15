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
func generateStockPricesForCompanies(companies []financial.Company, exchanges []financial.Exchange, numPricesPerCompany int, priceIDStart int64) []financial.DailyStockPrice {
	prices := make([]financial.DailyStockPrice, 0, len(companies)*numPricesPerCompany)
	priceIDCounter := priceIDStart

	for _, company := range companies {
		lastClose := gf.Float64Range(20, 500)
		date := time.Now().AddDate(-NumYearsOfData, 0, 0)

		for i := 0; i < numPricesPerCompany; i++ {
			openPrice := lastClose * (1 + (rand.Float64()-0.5)*0.1)
			highPrice := openPrice * (1 + rand.Float64()*0.05)
			lowPrice := openPrice * (1 - rand.Float64()*0.05)
			closePrice := (highPrice + lowPrice) / 2 * (1 + (rand.Float64()-0.5)*0.02)

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
	if numPrices <= 0 || len(companies) == 0 || len(exchanges) == 0 {
		return nil
	}

	numWorkers := runtime.NumCPU()
	companyChunks := make([][]financial.Company, numWorkers)
	chunkSize := (len(companies) + numWorkers - 1) / numWorkers

	for i := 0; i < numWorkers; i++ {
		start := i * chunkSize
		end := start + chunkSize
		if end > len(companies) {
			end = len(companies)
		}
		if start >= end {
			companyChunks[i] = []financial.Company{}
		} else {
			companyChunks[i] = companies[start:end]
		}
	}

	var wg sync.WaitGroup
	resultsChan := make(chan []financial.DailyStockPrice, numWorkers)

	avgPricesPerCompany := numPrices / len(companies)
	priceIDOffset := int64(1)

	fmt.Println("Starting concurrent generation of stock prices...")
	for i := 0; i < numWorkers; i++ {
		if len(companyChunks[i]) > 0 {
			wg.Add(1)
			go func(chunk []financial.Company, startID int64) {
				defer wg.Done()
				resultsChan <- generateStockPricesForCompanies(chunk, exchanges, avgPricesPerCompany, startID)
			}(companyChunks[i], priceIDOffset)
			priceIDOffset += int64(len(companyChunks[i]) * avgPricesPerCompany)
		}
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
	Companies          int
	Exchanges          int
	DailyStockPrices   int
}

func GenerateFinancialModelData(counts FinancialRowCounts, companies []financial.Company, exchanges []financial.Exchange, format string, outputDir string) error {
	// Generate and write daily stock prices concurrently
	if err := generateAndWriteDailyStockPricesConcurrently(counts.DailyStockPrices, companies, exchanges, format, outputDir); err != nil {
		return fmt.Errorf("error generating daily stock prices: %w", err)
	}

	return nil
}
