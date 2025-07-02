package financial

import "time"

type Company struct {
	CompanyID    int    `json:"company_id" parquet:"company_id"`
	CompanyName  string `json:"company_name" parquet:"company_name"`
	TickerSymbol string `json:"ticker_symbol" parquet:"ticker_symbol"`
	Sector       string `json:"sector" parquet:"sector"`
}

type Exchange struct {
	ExchangeID   int    `json:"exchange_id" parquet:"exchange_id"`
	ExchangeName string `json:"exchange_name" parquet:"exchange_name"`
	Country      string `json:"country" parquet:"country"`
}

type DailyStockPrice struct {
	PriceID    int64     `json:"price_id" parquet:"price_id"`
	Date       time.Time `json:"date" parquet:"date"`
	CompanyID  int       `json:"company_id" parquet:"company_id"`
	ExchangeID int       `json:"exchange_id" parquet:"exchange_id"`
	OpenPrice  float64   `json:"open_price" parquet:"open_price"`
	HighPrice  float64   `json:"high_price" parquet:"high_price"`
	LowPrice   float64   `json:"low_price" parquet:"low_price"`
	ClosePrice float64   `json:"close_price" parquet:"close_price"`
	Volume     int       `json:"volume" parquet:"volume"`
}
