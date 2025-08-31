package ecommerce

import "time"

type Customer struct {
	CustomerID int    `json:"customer_id" parquet:"customer_id"`
	FirstName  string `json:"first_name" parquet:"first_name"`
	LastName   string `json:"last_name" parquet:"last_name"`
	Email      string `json:"email" parquet:"email"`
}

type CustomerAddress struct {
	AddressID   int    `json:"address_id" parquet:"address_id"`
	CustomerID  int    `json:"customer_id" parquet:"customer_id"`
	AddressType string `json:"address_type" parquet:"address_type"`
	Address     string `json:"address" parquet:"address"`
	City        string `json:"city" parquet:"city"`
	State       string `json:"state" parquet:"state"`
	Zip         string `json:"zip" parquet:"zip"`
	Country     string `json:"country" parquet:"country"`
}

type Supplier struct {
	SupplierID   int    `json:"supplier_id" parquet:"supplier_id"`
	SupplierName string `json:"supplier_name" parquet:"supplier_name"`
	Country      string `json:"country" parquet:"country"`
}

type ProductCategory struct {
	CategoryID   int    `json:"category_id" parquet:"category_id"`
	CategoryName string `json:"category_name" parquet:"category_name"`
}

type Product struct {
	ProductID   int     `json:"product_id" parquet:"product_id"`
	SupplierID  int     `json:"supplier_id" parquet:"supplier_id"`
	ProductName string  `json:"product_name" parquet:"product_name"`
	CategoryID  int     `json:"category_id" parquet:"category_id"`
	BasePrice   float64 `json:"base_price" parquet:"base_price"`
}

type OrderHeader struct {
	OrderID           int       `json:"order_id" parquet:"order_id"`
	CustomerID        int       `json:"customer_id" parquet:"customer_id"`
	ShippingAddressID int       `json:"shipping_address_id" parquet:"shipping_address_id"`
	BillingAddressID  int       `json:"billing_address_id" parquet:"billing_address_id"`
	OrderTimestamp    time.Time `json:"order_timestamp" parquet:"order_timestamp"`
	OrderStatus       string    `json:"order_status" parquet:"order_status"`
}

type OrderItem struct {
	OrderItemID int     `json:"order_item_id" parquet:"order_item_id"`
	OrderID     int     `json:"order_id" parquet:"order_id"`
	ProductID   int     `json:"product_id" parquet:"product_id"`
	Quantity    int     `json:"quantity" parquet:"quantity"`
	UnitPrice   float64 `json:"unit_price" parquet:"unit_price"`
	Discount    float64 `json:"discount" parquet:"discount"`
}

type ProductDetails struct {
	BasePrice float64
}
