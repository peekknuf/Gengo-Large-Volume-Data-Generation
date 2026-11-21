package schema

import (
	"fmt"
	"reflect"

	"github.com/peekknuf/Gengo/internal/models/ecommerce"
)

type Column struct {
	Name         string
	PostgresType string
}

type ForeignKey struct {
	Column       string
	ParentTable  string
	ParentColumn string
}

type Table struct {
	Name        string
	Columns     []Column
	PrimaryKey  string
	ForeignKeys []ForeignKey
}

// ECommerce derives field order and names from the same structs used by the
// serializers. The order-header CSV encoder has one explicit epoch encoding.
func ECommerce() ([]Table, error) {
	definitions := []struct {
		name    string
		row     any
		foreign []ForeignKey
	}{
		{"dim_customers", ecommerce.Customer{}, nil},
		{"dim_customer_addresses", ecommerce.CustomerAddress{}, []ForeignKey{{"customer_id", "dim_customers", "customer_id"}}},
		{"dim_suppliers", ecommerce.Supplier{}, nil},
		{"dim_product_categories", ecommerce.ProductCategory{}, nil},
		{"dim_products", ecommerce.Product{}, []ForeignKey{{"supplier_id", "dim_suppliers", "supplier_id"}, {"category_id", "dim_product_categories", "category_id"}}},
		{"fact_orders_header", ecommerce.OrderHeader{}, []ForeignKey{{"customer_id", "dim_customers", "customer_id"}, {"shipping_address_id", "dim_customer_addresses", "address_id"}, {"billing_address_id", "dim_customer_addresses", "address_id"}}},
		{"fact_order_items", ecommerce.OrderItem{}, []ForeignKey{{"order_id", "fact_orders_header", "order_id"}, {"product_id", "dim_products", "product_id"}}},
	}
	tables := make([]Table, 0, len(definitions))
	for _, definition := range definitions {
		table := Table{Name: definition.name, ForeignKeys: definition.foreign}
		row := reflect.TypeOf(definition.row)
		for i := 0; i < row.NumField(); i++ {
			field := row.Field(i)
			column := Column{Name: field.Tag.Get("json")}
			switch {
			case definition.name == "fact_orders_header" && field.Name == "OrderTimestamp":
				column.Name = "order_timestamp_unix"
				column.PostgresType = "bigint"
			case field.Type.Kind() == reflect.Int:
				column.PostgresType = "integer"
			case field.Type.Kind() == reflect.Int64:
				column.PostgresType = "bigint"
			case field.Type.Kind() == reflect.Float64:
				column.PostgresType = "double precision"
			case field.Type.Kind() == reflect.String:
				column.PostgresType = "text"
			default:
				return nil, fmt.Errorf("unsupported PostgreSQL mapping for %s.%s", definition.name, field.Name)
			}
			if i == 0 {
				table.PrimaryKey = column.Name
			}
			table.Columns = append(table.Columns, column)
		}
		tables = append(tables, table)
	}
	return tables, nil
}
