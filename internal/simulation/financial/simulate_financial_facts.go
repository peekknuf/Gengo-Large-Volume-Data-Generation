package financial

import (
	"fmt"
	"math/rand"
	"time"

	gf "github.com/brianvoe/gofakeit/v6"
	"github.com/peekknuf/Gengo/internal/formats"
	"github.com/peekknuf/Gengo/internal/models/financial"
	"github.com/peekknuf/Gengo/internal/utils"
)

const NumYearsOfData = 5

func generateAndWriteDailyStockPrices(numPrices int, companies []financial.Company, exchanges []financial.Exchange, format string, outputDir string) error {
	if numPrices <= 0 || len(companies) == 0 || len(exchanges) == 0 {
		return nil
	}

	prices := make([]financial.DailyStockPrice, 0, numPrices)
	priceIDCounter := int64(1)

	avgPricesPerCompany := numPrices / len(companies)

	for _, company := range companies {
		lastClose := gf.Float64Range(20, 500)
		date := time.Now().AddDate(-NumYearsOfData, 0, 0)

		for i := 0; i < avgPricesPerCompany; i++ {
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

			utils.PrintProgress(len(prices), numPrices, "Generating Stock Prices")
			}
			if len(prices) >= numPrices {
				break
			}
		}
	utils.PrintProgress(numPrices, numPrices, "Generating Stock Prices")
	fmt.Println()

	return formats.WriteSliceData(prices, "fact_daily_stock_prices", format, outputDir)
}

type FinancialRowCounts struct {
	Companies          int
	Exchanges          int
	DailyStockPrices   int
}

func GenerateFinancialModelData(counts FinancialRowCounts, companies []financial.Company, exchanges []financial.Exchange, format string, outputDir string) error {

	// Generate and write daily stock prices
	if err := generateAndWriteDailyStockPrices(counts.DailyStockPrices, companies, exchanges, format, outputDir); err != nil {
		return fmt.Errorf("error generating daily stock prices: %w", err)
	}

	return nil
}