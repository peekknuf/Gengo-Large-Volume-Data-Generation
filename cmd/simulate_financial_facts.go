// cmd/simulate_financial_facts.go
package cmd

import (
	"fmt"
	"math/rand"
	"time"

	gf "github.com/brianvoe/gofakeit/v6"
)

func generateAndWriteDailyStockPrices(numPrices int, companies []Company, exchanges []Exchange, format string, outputDir string) error {
	if numPrices <= 0 || len(companies) == 0 || len(exchanges) == 0 {
		return nil // No prices to generate or missing dimensions
	}

	prices := make([]DailyStockPrice, 0, numPrices)
	priceIDCounter := int64(1)

	// Calculate total trading days needed across all companies to reach numPrices
	// This is a rough estimate to distribute prices across companies
	avgPricesPerCompany := numPrices / len(companies)

	for _, company := range companies {
		lastClose := gf.Float64Range(20, 500) // Initial stock price
		date := time.Now().AddDate(-NumYearsOfData, 0, 0)

		// Generate prices for this company up to its share of total prices
		for i := 0; i < avgPricesPerCompany; i++ {
			openPrice := lastClose * (1 + (rand.Float64()-0.5)*0.1) // Fluctuate up to 5%
			highPrice := openPrice * (1 + rand.Float64()*0.05)
			lowPrice := openPrice * (1 - rand.Float64()*0.05)
			closePrice := (highPrice + lowPrice) / 2 * (1 + (rand.Float64()-0.5)*0.02)

			prices = append(prices, DailyStockPrice{
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

			// Update progress after each price is generated
			printProgress(len(prices), numPrices, "Generating Stock Prices")
			}
			if len(prices) >= numPrices {
				break // Break from outer loop
			}
		}
	printProgress(numPrices, numPrices, "Generating Stock Prices") // Ensure 100% is printed
	fmt.Println() // Ensure a newline after progress bar

	// Write the generated prices to file
	return writeSliceData(prices, "fact_daily_stock_prices", format, outputDir)
}

