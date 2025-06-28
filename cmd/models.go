// cmd/models.go
package cmd

import "time"

// --- E-commerce Model ---

// Customer represents the dim_customers table
type Customer struct {
	CustomerID int    `json:"customer_id" parquet:"customer_id"`
	FirstName  string `json:"first_name" parquet:"first_name"`
	LastName   string `json:"last_name" parquet:"last_name"`
	Email      string `json:"email" parquet:"email"`
}

// CustomerAddress represents the dim_customer_addresses table
type CustomerAddress struct {
	AddressID   int    `json:"address_id" parquet:"address_id"`
	CustomerID  int    `json:"customer_id" parquet:"customer_id"` // FK to dim_customers
	AddressType string `json:"address_type" parquet:"address_type"` // e.g., "shipping", "billing"
	Address     string `json:"address" parquet:"address"`
	City        string `json:"city" parquet:"city"`
	State       string `json:"state" parquet:"state"`
	Zip         string `json:"zip" parquet:"zip"`
	Country     string `json:"country" parquet:"country"`
}

// Supplier represents the dim_suppliers table
type Supplier struct {
	SupplierID   int    `json:"supplier_id" parquet:"supplier_id"`
	SupplierName string `json:"supplier_name" parquet:"supplier_name"`
	Country      string `json:"country" parquet:"country"`
}

// Product represents the dim_products table
type Product struct {
	ProductID       int     `json:"product_id" parquet:"product_id"`
	SupplierID      int     `json:"supplier_id" parquet:"supplier_id"` // FK to dim_suppliers
	ProductName     string  `json:"product_name" parquet:"product_name"`
	ProductCategory string  `json:"product_category" parquet:"product_category"`
	BasePrice       float64 `json:"base_price" parquet:"base_price"`
}

// OrderHeader represents the fact_orders_header table
type OrderHeader struct {
	OrderID           int       `json:"order_id" parquet:"order_id"`
	CustomerID        int       `json:"customer_id" parquet:"customer_id"`               // FK to dim_customers
	ShippingAddressID int       `json:"shipping_address_id" parquet:"shipping_address_id"` // FK to dim_customer_addresses
	BillingAddressID  int       `json:"billing_address_id" parquet:"billing_address_id"`   // FK to dim_customer_addresses
	OrderTimestamp    time.Time `json:"order_timestamp" parquet:"order_timestamp"`
	OrderStatus       string    `json:"order_status" parquet:"order_status"`
	TotalOrderAmount  float64   `json:"total_order_amount" parquet:"total_order_amount"` // Calculated from order items
}

// OrderItem represents the fact_order_items table
type OrderItem struct {
	OrderItemID int     `json:"order_item_id" parquet:"order_item_id"`
	OrderID     int     `json:"order_id" parquet:"order_id"`         // FK to fact_orders_header
	ProductID   int     `json:"product_id" parquet:"product_id"`     // FK to dim_products
	Quantity    int     `json:"quantity" parquet:"quantity"`
	UnitPrice   float64 `json:"unit_price" parquet:"unit_price"`   // Price at time of sale
	Discount    float64 `json:"discount" parquet:"discount"`       // 0.0 to 1.0
	TotalPrice  float64 `json:"total_price" parquet:"total_price"` // Calculated: Quantity * UnitPrice * (1 - Discount)
}

// ProductDetails is used to pass essential product info for order generation.
type ProductDetails struct {
	BasePrice float64
}

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

