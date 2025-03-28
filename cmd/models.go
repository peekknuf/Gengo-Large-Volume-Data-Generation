// cmd/models.go
package cmd

import "time"

// Customer represents the dim_Customers table
type Customer struct {
	CustomerID int    `json:"customer_id" parquet:"customer_id"` // Primary Key
	FirstName  string `json:"first_name" parquet:"first_name"`
	LastName   string `json:"last_name" parquet:"last_name"`
	Email      string `json:"email" parquet:"email"`
	// We might add FKs to primary address/location later if needed
}

// Product represents the dim_Products table
type Product struct {
	ProductID       int     `json:"product_id" parquet:"product_id"` // Primary Key
	ProductName     string  `json:"product_name" parquet:"product_name"`
	ProductCategory string  `json:"product_category" parquet:"product_category"`
	Company         string  `json:"company" parquet:"company"`       // Supplier/Manufacturer
	BasePrice       float64 `json:"base_price" parquet:"base_price"` // Canonical price
}

// Location represents the dim_Locations table
type Location struct {
	LocationID int    `json:"location_id" parquet:"location_id"` // Primary Key
	Address    string `json:"address" parquet:"address"`
	City       string `json:"city" parquet:"city"`
	State      string `json:"state" parquet:"state"`
	Zip        string `json:"zip" parquet:"zip"`
	Country    string `json:"country" parquet:"country"`
}

// OrderFact represents the fact_Orders table
type OrderFact struct {
	OrderID            int       `json:"order_id" parquet:"order_id"` // Primary Key for this fact row (can be unique or just sequence)
	OrderTimestamp     time.Time `json:"order_timestamp" parquet:"order_timestamp"`
	CustomerID         int       `json:"customer_id" parquet:"customer_id"`                   // Foreign Key -> dim_Customers
	ProductID          int       `json:"product_id" parquet:"product_id"`                     // Foreign Key -> dim_Products
	ShippingLocationID int       `json:"shipping_location_id" parquet:"shipping_location_id"` // Foreign Key -> dim_Locations
	BillingLocationID  int       `json:"billing_location_id" parquet:"billing_location_id"`   // Foreign Key -> dim_Locations (can be same as shipping)
	Quantity           int       `json:"quantity" parquet:"quantity"`
	UnitPrice          float64   `json:"unit_price" parquet:"unit_price"`   // Price at the time of sale (from Product.BasePrice usually)
	Discount           float64   `json:"discount" parquet:"discount"`       // 0.0 to 1.0
	TotalPrice         float64   `json:"total_price" parquet:"total_price"` // Calculated: Quantity * UnitPrice * (1 - Discount)
	OrderStatus        string    `json:"order_status" parquet:"order_status"`
	PaymentMethod      string    `json:"payment_method" parquet:"payment_method"`
}

// --- Helper Struct for passing product info ---

// ProductDetails might be needed to pass more than just ID from product generation
// If we only need BasePrice for order calculation, a map[int]float64 might suffice.
// If we might need other product attributes during order gen, use this struct.
type ProductDetails struct {
	BasePrice float64
	// Add other fields if needed during order generation (e.g., Category for discount logic)
}

// Optional: TimeDimension represents dim_Time (can be generated later or on-the-fly)
// type TimeDimension struct {
// 	TimestampKey  int64 // e.g., UnixNano or YYYYMMDDHHMMSS
// 	FullTimestamp time.Time
// 	Year          int
// 	Month         int
// 	Day           int
// 	Hour          int
// 	Minute        int
// 	Second        int
// 	Weekday       string
// 	// etc.
// }
