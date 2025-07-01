package financial

import "time"

// --- Financial Model ---

// Company represents the dim_companies table
type Company struct {
	CompanyID    int    `json:"company_id" parquet:"company_id"`
	CompanyName  string `json:"company_name" parquet:"company_name"`
	TickerSymbol string `json:"ticker_symbol" parquet:"ticker_symbol"`
	Sector       string `json:"sector" parquet:"sector"`
}

// Exchange represents the dim_exchanges table
type Exchange struct {
	ExchangeID   int    `json:"exchange_id" parquet:"exchange_id"`
	ExchangeName string `json:"exchange_name" parquet:"exchange_name"`
	Country      string `json:"country" parquet:"country"`
}

// DailyStockPrice represents the fact_daily_stock_prices table
type DailyStockPrice struct {
	PriceID    int64     `json:"price_id" parquet:"price_id"`
	Date       time.Time `json:"date" parquet:"date"`
	CompanyID  int       `json:"company_id" parquet:"company_id"`   // FK to dim_companies
	ExchangeID int       `json:"exchange_id" parquet:"exchange_id"` // FK to dim_exchanges
	OpenPrice  float64   `json:"open_price" parquet:"open_price"`
	HighPrice  float64   `json:"high_price" parquet:"high_price"`
	LowPrice   float64   `json:"low_price" parquet:"low_price"`
	ClosePrice float64   `json:"close_price" parquet:"close_price"`
	Volume     int       `json:"volume" parquet:"volume"`
}
