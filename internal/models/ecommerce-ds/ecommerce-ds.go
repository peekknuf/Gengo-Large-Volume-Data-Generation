package ecommerceds

import "time"

// StoreSales represents a single lineitem for a sale made through the store channel.
type StoreSales struct {
	SS_SoldDateSK          int64   `csv:"ss_sold_date_sk"`
	SS_SoldTimeSK          int64   `csv:"ss_sold_time_sk"`
	SS_ItemSK              int64   `csv:"ss_item_sk"`
	SS_CustomerSK          int64   `csv:"ss_customer_sk"`
	SS_CDemoSK             int64   `csv:"ss_cdemo_sk"`
	SS_HDemoSK             int64   `csv:"ss_hdemo_sk"`
	SS_AddrSK              int64   `csv:"ss_addr_sk"`
	SS_StoreSK             int64   `csv:"ss_store_sk"`
	SS_PromoSK             int64   `csv:"ss_promo_sk"`
	SS_TicketNumber        int64   `csv:"ss_ticket_number"`
	SS_Quantity            int     `csv:"ss_quantity"`
	SS_WholesaleCost       float64 `csv:"ss_wholesale_cost"`
	SS_ListPrice           float64 `csv:"ss_list_price"`
	SS_SalesPrice          float64 `csv:"ss_sales_price"`
	SS_ExtDiscountAmt      float64 `csv:"ss_ext_discount_amt"`
	SS_ExtSalesPrice       float64 `csv:"ss_ext_sales_price"`
	SS_ExtWholesaleCost    float64 `csv:"ss_ext_wholesale_cost"`
	SS_ExtListPrice        float64 `csv:"ss_ext_list_price"`
	SS_ExtTax              float64 `csv:"ss_ext_tax"`
	SS_CouponAmt           float64 `csv:"ss_coupon_amt"`
	SS_NetPaid             float64 `csv:"ss_net_paid"`
	SS_NetPaidIncTax       float64 `csv:"ss_net_paid_inc_tax"`
	SS_NetProfit           float64 `csv:"ss_net_profit"`
}

// StoreReturns represents a single lineitem for the return of an item sold through the store channel.
type StoreReturns struct {
	SR_ReturnedDateSK   int64   `csv:"sr_returned_date_sk"`
	SR_ReturnTimeSK     int64   `csv:"sr_return_time_sk"`
	SR_ItemSK           int64   `csv:"sr_item_sk"`
	SR_CustomerSK       int64   `csv:"sr_customer_sk"`
	SR_CDemoSK          int64   `csv:"sr_cdemo_sk"`
	SR_HDemoSK          int64   `csv:"sr_hdemo_sk"`
	SR_AddrSK           int64   `csv:"sr_addr_sk"`
	SR_StoreSK          int64   `csv:"sr_store_sk"`
	SR_ReasonSK         int64   `csv:"sr_reason_sk"`
	SR_TicketNumber     int64   `csv:"sr_ticket_number"`
	SR_ReturnQuantity   int     `csv:"sr_return_quantity"`
	SR_ReturnAmt        float64 `csv:"sr_return_amt"`
	SR_ReturnTax        float64 `csv:"sr_return_tax"`
	SR_ReturnAmtIncTax  float64 `csv:"sr_return_amt_inc_tax"`
	SR_Fee              float64 `csv:"sr_fee"`
	SR_ReturnShipCost   float64 `csv:"sr_return_ship_cost"`
	SR_RefundedCash     float64 `csv:"sr_refunded_cash"`
	SR_ReversedCharge   float64 `csv:"sr_reversed_charge"`
	SR_StoreCredit      float64 `csv:"sr_store_credit"`
	SR_NetLoss          float64 `csv:"sr_net_loss"`
}

// CatalogSales represents a single lineitem for a sale made through the catalog channel.
type CatalogSales struct {
	CS_SoldDateSK          int64   `csv:"cs_sold_date_sk"`
	CS_SoldTimeSK          int64   `csv:"cs_sold_time_sk"`
	CS_ShipDateSK          int64   `csv:"cs_ship_date_sk"`
	CS_BillCustomerSK      int64   `csv:"cs_bill_customer_sk"`
	CS_BillCDemoSK         int64   `csv:"cs_bill_cdemo_sk"`
	CS_BillHDemoSK         int64   `csv:"cs_bill_hdemo_sk"`
	CS_BillAddrSK          int64   `csv:"cs_bill_addr_sk"`
	CS_ShipCustomerSK      int64   `csv:"cs_ship_customer_sk"`
	CS_ShipCDemoSK         int64   `csv:"cs_ship_cdemo_sk"`
	CS_ShipHDemoSK         int64   `csv:"cs_ship_hdemo_sk"`
	CS_ShipAddrSK          int64   `csv:"cs_ship_addr_sk"`
	CS_CallCenterSK        int64   `csv:"cs_call_center_sk"`
	CS_CatalogPageSK       int64   `csv:"cs_catalog_page_sk"`
	CS_ShipModeSK          int64   `csv:"cs_ship_mode_sk"`
	CS_WarehouseSK         int64   `csv:"cs_warehouse_sk"`
	CS_ItemSK              int64   `csv:"cs_item_sk"`
	CS_PromoSK             int64   `csv:"cs_promo_sk"`
	CS_OrderNumber         int64   `csv:"cs_order_number"`
	CS_Quantity            int     `csv:"cs_quantity"`
	CS_WholesaleCost       float64 `csv:"cs_wholesale_cost"`
	CS_ListPrice           float64 `csv:"cs_list_price"`
	CS_SalesPrice          float64 `csv:"cs_sales_price"`
	CS_ExtDiscountAmt      float64 `csv:"cs_ext_discount_amt"`
	CS_ExtSalesPrice       float64 `csv:"cs_ext_sales_price"`
	CS_ExtWholesaleCost    float64 `csv:"cs_ext_wholesale_cost"`
	CS_ExtListPrice        float64 `csv:"cs_ext_list_price"`
	CS_ExtTax              float64 `csv:"cs_ext_tax"`
	CS_CouponAmt           float64 `csv:"cs_coupon_amt"`
	CS_ExtShipCost         float64 `csv:"cs_ext_ship_cost"`
	CS_NetPaid             float64 `csv:"cs_net_paid"`
	CS_NetPaidIncTax       float64 `csv:"cs_net_paid_inc_tax"`
	CS_NetPaidIncShip      float64 `csv:"cs_net_paid_inc_ship"`
	CS_NetPaidIncShipTax   float64 `csv:"cs_net_paid_inc_ship_tax"`
	CS_NetProfit           float64 `csv:"cs_net_profit"`
}

// CatalogReturns represents a single lineitem for the return of an item sold through the catalog channel.
type CatalogReturns struct {
	CR_ReturnedDateSK      int64   `csv:"cr_returned_date_sk"`
	CR_ReturnedTimeSK      int64   `csv:"cr_returned_time_sk"`
	CR_ItemSK              int64   `csv:"cr_item_sk"`
	CR_RefundCustomerSK    int64   `csv:"cr_refund_customer_sk"`
	CR_RefundCDemoSK       int64   `csv:"cr_refund_cdemo_sk"`
	CR_RefundHDemoSK       int64   `csv:"cr_refund_hdemo_sk"`
	CR_RefundAddrSK        int64   `csv:"cr_refund_addr_sk"`
	CR_ReturningCustomerSK int64   `csv:"cr_returning_customer_sk"`
	CR_ReturningCDemoSK    int64   `csv:"cr_returning_cdemo_sk"`
	CR_ReturningHDemoSK    int64   `csv:"cr_returning_hdemo_sk"`
	CR_ReturningAddrSK     int64   `csv:"cr_returning_addr_sk"`
	CR_CallCenterSK        int64   `csv:"cr_call_center_sk"`
	CR_CatalogPageSK       int64   `csv:"cr_catalog_page_sk"`
	CR_ShipModeSK          int64   `csv:"cr_ship_mode_sk"`
	CR_WarehouseSK         int64   `csv:"cr_warehouse_sk"`
	CR_ReasonSK            int64   `csv:"cr_reason_sk"`
	CR_OrderNumber         int64   `csv:"cr_order_number"`
	CR_ReturnQuantity      int     `csv:"cr_return_quantity"`
	CR_ReturnAmount        float64 `csv:"cr_return_amount"`
	CR_ReturnTax           float64 `csv:"cr_return_tax"`
	CR_ReturnAmtIncTax     float64 `csv:"cr_return_amt_inc_tax"`
	CR_Fee                 float64 `csv:"cr_fee"`
	CR_ReturnShipCost      float64 `csv:"cr_return_ship_cost"`
	CR_RefundedCash        float64 `csv:"cr_refunded_cash"`
	CR_ReversedCharge      float64 `csv:"cr_reversed_charge"`
	CR_StoreCredit         float64 `csv:"cr_store_credit"`
	CR_NetLoss             float64 `csv:"cr_net_loss"`
}

// WebSales represents a single lineitem for a sale made through the web channel.
type WebSales struct {
	WS_SoldDateSK          int64   `csv:"ws_sold_date_sk"`
	WS_SoldTimeSK          int64   `csv:"ws_sold_time_sk"`
	WS_ShipDateSK          int64   `csv:"ws_ship_date_sk"`
	WS_ItemSK              int64   `csv:"ws_item_sk"`
	WS_BillCustomerSK      int64   `csv:"ws_bill_customer_sk"`
	WS_BillCDemoSK         int64   `csv:"ws_bill_cdemo_sk"`
	WS_BillHDemoSK         int64   `csv:"ws_bill_hdemo_sk"`
	WS_BillAddrSK          int64   `csv:"ws_bill_addr_sk"`
	WS_ShipCustomerSK      int64   `csv:"ws_ship_customer_sk"`
	WS_ShipCDemoSK         int64   `csv:"ws_ship_cdemo_sk"`
	WS_ShipHDemoSK         int64   `csv:"ws_ship_hdemo_sk"`
	WS_ShipAddrSK          int64   `csv:"ws_ship_addr_sk"`
	WS_WebPageSK           int64   `csv:"ws_web_page_sk"`
	WS_WebSiteSK           int64   `csv:"ws_web_site_sk"`
	WS_ShipModeSK          int64   `csv:"ws_ship_mode_sk"`
	WS_WarehouseSK         int64   `csv:"ws_warehouse_sk"`
	WS_PromoSK             int64   `csv:"ws_promo_sk"`
	WS_OrderNumber         int64   `csv:"ws_order_number"`
	WS_Quantity            int     `csv:"ws_quantity"`
	WS_WholesaleCost       float64 `csv:"ws_wholesale_cost"`
	WS_ListPrice           float64 `csv:"ws_list_price"`
	WS_SalesPrice          float64 `csv:"ws_sales_price"`
	WS_ExtDiscountAmt      float64 `csv:"ws_ext_discount_amt"`
	WS_ExtSalesPrice       float64 `csv:"ws_ext_sales_price"`
	WS_ExtWholesaleCost    float64 `csv:"ws_ext_wholesale_cost"`
	WS_ExtListPrice        float64 `csv:"ws_ext_list_price"`
	WS_ExtTax              float64 `csv:"ws_ext_tax"`
	WS_CouponAmt           float64 `csv:"ws_coupon_amt"`
	WS_ExtShipCost         float64 `csv:"ws_ext_ship_cost"`
	WS_NetPaid             float64 `csv:"ws_net_paid"`
	WS_NetPaidIncTax       float64 `csv:"ws_net_paid_inc_tax"`
	WS_NetPaidIncShip      float64 `csv:"ws_net_paid_inc_ship"`
	WS_NetPaidIncShipTax   float64 `csv:"ws_net_paid_inc_ship_tax"`
	WS_NetProfit           float64 `csv:"ws_net_profit"`
}

// WebReturns represents a single lineitem for the return of an item sold through the web sales channel.
type WebReturns struct {
	WR_ReturnedDateSK      int64   `csv:"wr_returned_date_sk"`
	WR_ReturnedTimeSK      int64   `csv:"wr_returned_time_sk"`
	WR_ItemSK              int64   `csv:"wr_item_sk"`
	WR_RefundCustomerSK    int64   `csv:"wr_refund_customer_sk"`
	WR_RefundCDemoSK       int64   `csv:"wr_refund_cdemo_sk"`
	WR_RefundHDemoSK       int64   `csv:"wr_refund_hdemo_sk"`
	WR_RefundAddrSK        int64   `csv:"wr_refund_addr_sk"`
	WR_ReturningCustomerSK int64   `csv:"wr_returning_customer_sk"`
	WR_ReturningCDemoSK    int64   `csv:"wr_returning_cdemo_sk"`
	WR_ReturningHDemoSK    int64   `csv:"wr_returning_hdemo_sk"`
	WR_ReturningAddrSK     int64   `csv:"wr_returning_addr_sk"`
	WR_WebPageSK           int64   `csv:"wr_web_page_sk"`
	WR_ReasonSK            int64   `csv:"wr_reason_sk"`
	WR_OrderNumber         int64   `csv:"wr_order_number"`
	WR_ReturnQuantity      int     `csv:"wr_return_quantity"`
	WR_ReturnAmt           float64 `csv:"wr_return_amt"`
	WR_ReturnTax           float64 `csv:"wr_return_tax"`
	WR_ReturnAmtIncTax     float64 `csv:"wr_return_amt_inc_tax"`
	WR_Fee                 float64 `csv:"wr_fee"`
	WR_ReturnShipCost      float64 `csv:"wr_return_ship_cost"`
	WR_RefundedCash        float64 `csv:"wr_refunded_cash"`
	WR_ReversedCharge      float64 `csv:"wr_reversed_charge"`
	WR_AccountCredit       float64 `csv:"wr_account_credit"`
	WR_NetLoss             float64 `csv:"wr_net_loss"`
}

// Inventory represents the quantity of an item on-hand at a given warehouse during a specific week.
type Inventory struct {
	Inv_DateSK         int64 `csv:"inv_date_sk"`
	Inv_ItemSK         int64 `csv:"inv_item_sk"`
	Inv_WarehouseSK    int64 `csv:"inv_warehouse_sk"`
	Inv_QuantityOnHand int   `csv:"inv_quantity_on_hand"`
}

// Store represents details of a store.
type Store struct {
	S_StoreSK         int64   `csv:"s_store_sk"`
	S_StoreID         string  `csv:"s_store_id"`
	S_RecStartDate    string  `csv:"s_rec_start_date"`
	S_RecEndDate      string  `csv:"s_rec_end_date"`
	S_ClosedDateSK    int64   `csv:"s_closed_date_sk"`
	S_StoreName       string  `csv:"s_store_name"`
	S_NumberOfEmployees int     `csv:"s_number_of_employees"`
	S_FloorSpace      int     `csv:"s_floor_space"`
	S_Hours           string  `csv:"s_hours"`
	S_Manager         string  `csv:"s_manager"`
	S_MarketID        int     `csv:"s_market_id"`
	S_GeographyClass  string  `csv:"s_geography_class"`
	S_MarketDesc      string  `csv:"s_market_desc"`
	S_MarketManager   string  `csv:"s_market_manager"`
	S_DivisionID      int     `csv:"s_division_id"`
	S_DivisionName    string  `csv:"s_division_name"`
	S_CompanyID       int     `csv:"s_company_id"`
	S_CompanyName     string  `csv:"s_company_name"`
	S_StreetNumber    string  `csv:"s_street_number"`
	S_StreetName      string  `csv:"s_street_name"`
	S_StreetType      string  `csv:"s_street_type"`
	S_SuiteNumber     string  `csv:"s_suite_number"`
	S_City            string  `csv:"s_city"`
	S_County          string  `csv:"s_county"`
	S_State           string  `csv:"s_state"`
	S_Zip             string  `csv:"s_zip"`
	S_Country         string  `csv:"s_country"`
	S_GmtOffset       float64 `csv:"s_gmt_offset"`
	S_TaxPercentage   float64 `csv:"s_tax_percentage"`
}

// CallCenter represents details of a call center.
type CallCenter struct {
	CC_CallCenterSK   int64   `csv:"cc_call_center_sk"`
	CC_CallCenterID   string  `csv:"cc_call_center_id"`
	CC_RecStartDate   string  `csv:"cc_rec_start_date"`
	CC_RecEndDate     string  `csv:"cc_rec_end_date"`
	CC_ClosedDateSK   int64   `csv:"cc_closed_date_sk"`
	CC_OpenDateSK     int64   `csv:"cc_open_date_sk"`
	CC_Name           string  `csv:"cc_name"`
	CC_Class          string  `csv:"cc_class"`
	CC_Employees      int     `csv:"cc_employees"`
	CC_SqFt           int     `csv:"cc_sq_ft"`
	CC_Hours          string  `csv:"cc_hours"`
	CC_Manager        string  `csv:"cc_manager"`
	CC_MktID          int     `csv:"cc_mkt_id"`
	CC_MktClass       string  `csv:"cc_mkt_class"`
	CC_MktDesc        string  `csv:"cc_mkt_desc"`
	CC_MarketManager  string  `csv:"cc_market_manager"`
	CC_Division       int     `csv:"cc_division"`
	CC_DivisionName   string  `csv:"cc_division_name"`
	CC_Company        int     `csv:"cc_company"`
	CC_CompanyName    string  `csv:"cc_company_name"`
	CC_StreetNumber   string  `csv:"cc_street_number"`
	CC_StreetName     string  `csv:"cc_street_name"`
	CC_StreetType     string  `csv:"cc_street_type"`
	CC_SuiteNumber    string  `csv:"cc_suite_number"`
	CC_City           string  `csv:"cc_city"`
	CC_County         string  `csv:"cc_county"`
	CC_State          string  `csv:"cc_state"`
	CC_Zip            string  `csv:"cc_zip"`
	CC_Country        string  `csv:"cc_country"`
	CC_GmtOffset      float64 `csv:"cc_gmt_offset"`
	CC_TaxPercentage  float64 `csv:"cc_tax_percentage"`
}

// CatalogPage represents details of a catalog page.
type CatalogPage struct {
	CP_CatalogPageSK     int64  `csv:"cp_catalog_page_sk"`
	CP_CatalogPageID     string `csv:"cp_catalog_page_id"`
	CP_StartDateSK       int64  `csv:"cp_start_date_sk"`
	CP_EndDateSK         int64  `csv:"cp_end_date_sk"`
	CP_Department        string `csv:"cp_department"`
	CP_CatalogNumber     int    `csv:"cp_catalog_number"`
	CP_CatalogPageNumber int    `csv:"cp_catalog_page_number"`
	CP_Description       string `csv:"cp_description"`
	CP_Type              string `csv:"cp_type"`
}

// WebSite represents details of a web site.
type WebSite struct {
	Web_SiteSK        int64   `csv:"web_site_sk"`
	Web_SiteID        string  `csv:"web_site_id"`
	Web_RecStartDate  string  `csv:"web_rec_start_date"`
	Web_RecEndDate    string  `csv:"web_rec_end_date"`
	Web_Name          string  `csv:"web_name"`
	Web_OpenDateSK    int64   `csv:"web_open_date_sk"`
	Web_CloseDateSK   int64   `csv:"web_close_date_sk"`
	Web_Class         string  `csv:"web_class"`
	Web_Manager       string  `csv:"web_manager"`
	Web_MktID         int     `csv:"web_mkt_id"`
	Web_MktClass      string  `csv:"web_mkt_class"`
	Web_MktDesc       string  `csv:"web_mkt_desc"`
	Web_MarketManager string  `csv:"web_market_manager"`
	Web_CompanyID     int     `csv:"web_company_id"`
	Web_CompanyName   string  `csv:"web_company_name"`
	Web_StreetNumber  string  `csv:"web_street_number"`
	Web_StreetName    string  `csv:"web_street_name"`
	Web_StreetType    string  `csv:"web_street_type"`
	Web_SuiteNumber   string  `csv:"web_suite_number"`
	Web_City          string  `csv:"web_city"`
	Web_County        string  `csv:"web_county"`
	Web_State         string  `csv:"web_state"`
	Web_Zip           string  `csv:"web_zip"`
	Web_Country       string  `csv:"web_country"`
	Web_GmtOffset     float64 `csv:"web_gmt_offset"`
	Web_TaxPercentage float64 `csv:"web_tax_percentage"`
}

// WebPage represents details of a web page within a web site.
type WebPage struct {
	WP_WebPageSK      int64   `csv:"wp_web_page_sk"`
	WP_WebPageID      string  `csv:"wp_web_page_id"`
	WP_RecStartDate   string  `csv:"wp_rec_start_date"`
	WP_RecEndDate     string  `csv:"wp_rec_end_date"`
	WP_CreationDateSK int64   `csv:"wp_creation_date_sk"`
	WP_AccessDateSK   int64   `csv:"wp_access_date_sk"`
	WP_AutogenFlag    string  `csv:"wp_autogen_flag"`
	WP_CustomerSK     int64   `csv:"wp_customer_sk"`
	WP_URL            string  `csv:"wp_url"`
	WP_Type           string  `csv:"wp_type"`
	WP_CharCount      int     `csv:"wp_char_count"`
	WP_LinkCount      int     `csv:"wp_link_count"`
	WP_ImageCount     int     `csv:"wp_image_count"`
	WP_MaxAdCount     int     `csv:"wp_max_ad_count"`
}

// Warehouse represents a warehouse where items are stocked.
type Warehouse struct {
	W_WarehouseSK      int64   `csv:"w_warehouse_sk"`
	W_WarehouseID      string  `csv:"w_warehouse_id"`
	W_WarehouseName    string  `csv:"w_warehouse_name"`
	W_WarehouseSqFt    int     `csv:"w_warehouse_sq_ft"`
	W_StreetNumber     string  `csv:"w_street_number"`
	W_StreetName       string  `csv:"w_street_name"`
	W_StreetType       string  `csv:"w_street_type"`
	W_SuiteNumber      string  `csv:"w_suite_number"`
	W_City             string  `csv:"w_city"`
	W_County           string  `csv:"w_county"`
	W_State            string  `csv:"w_state"`
	W_Zip              string  `csv:"w_zip"`
	W_Country          string  `csv:"w_country"`
	W_GmtOffset        float64 `csv:"w_gmt_offset"`
}

// Customer represents a customer.
type Customer struct {
	C_CustomerSK          int64  `csv:"c_customer_sk"`
	C_CustomerID          string `csv:"c_customer_id"`
	C_CurrentCDemoSK      int64  `csv:"c_current_cdemo_sk"`
	C_CurrentHDemoSK      int64  `csv:"c_current_hdemo_sk"`
	C_CurrentAddrSK       int64  `csv:"c_current_addr_sk"`
	C_FirstShiptoDateSK   int64  `csv:"c_first_shipto_date_sk"`
	C_FirstSalesDateSK    int64  `csv:"c_first_sales_date_sk"`
	C_Salutation          string `csv:"c_salutation"`
	C_FirstName           string `csv:"c_first_name"`
	C_LastName            string `csv:"c_last_name"`
	C_PreferredCustFlag   string `csv:"c_preferred_cust_flag"`
	C_BirthDay            int    `csv:"c_birth_day"`
	C_BirthMonth          int    `csv:"c_birth_month"`
	C_BirthYear           int    `csv:"c_birth_year"`
	C_BirthCountry        string `csv:"c_birth_country"`
	C_Login               string `csv:"c_login"`
	C_EmailAddress        string `csv:"c_email_address"`
	C_LastReviewDate      string `csv:"c_last_review_date"`
}

// CustomerAddress represents a unique customer address.
type CustomerAddress struct {
	CA_AddressSK     int64   `csv:"ca_address_sk"`
	CA_AddressID     string  `csv:"ca_address_id"`
	CA_StreetNumber  string  `csv:"ca_street_number"`
	CA_StreetName    string  `csv:"ca_street_name"`
	CA_StreetType    string  `csv:"ca_street_type"`
	CA_SuiteNumber   string  `csv:"ca_suite_number"`
	CA_City          string  `csv:"ca_city"`
	CA_County        string  `csv:"ca_county"`
	CA_State         string  `csv:"ca_state"`
	CA_Zip           string  `csv:"ca_zip"`
	CA_Country       string  `csv:"ca_country"`
	CA_GmtOffset     float64 `csv:"ca_gmt_offset"`
	CA_LocationType  string  `csv:"ca_location_type"`
}

// CustomerDemographics contains one row for each unique combination of customer demographic info.
type CustomerDemographics struct {
	CD_DemoSK              int64  `csv:"cd_demo_sk"`
	CD_Gender              string `csv:"cd_gender"`
	CD_MaritalStatus       string `csv:"cd_marital_status"`
	CD_EducationStatus     string `csv:"cd_education_status"`
	CD_PurchaseEstimate    int    `csv:"cd_purchase_estimate"`
	CD_CreditRating        string `csv:"cd_credit_rating"`
	CD_DepCount            int    `csv:"cd_dep_count"`
	CD_DepEmployedCount    int    `csv:"cd_dep_employed_count"`
	CD_DepCollegeCount     int    `csv:"cd_dep_college_count"`
}

// DateDim represents one calendar day.
type DateDim struct {
	D_DateSK           int64     `csv:"d_date_sk"`
	D_DateID           string    `csv:"d_date_id"`
	D_Date             time.Time `csv:"d_date"`
	D_MonthSeq         int       `csv:"d_month_seq"`
	D_WeekSeq          int       `csv:"d_week_seq"`
	D_QuarterSeq       int       `csv:"d_quarter_seq"`
	D_Year             int       `csv:"d_year"`
	D_Dow              int       `csv:"d_dow"`
	D_Moy              int       `csv:"d_moy"`
	D_Dom              int       `csv:"d_dom"`
	D_Qoy              int       `csv:"d_qoy"`
	D_FyYear           int       `csv:"d_fy_year"`
	D_FyQuarterSeq     int       `csv:"d_fy_quarter_seq"`
	D_FyWeekSeq        int       `csv:"d_fy_week_seq"`
	D_DayName          string    `csv:"d_day_name"`
	D_QuarterName      string    `csv:"d_quarter_name"`
	D_Holiday          string    `csv:"d_holiday"`
	D_Weekend          string    `csv:"d_weekend"`
	D_FollowingHoliday string    `csv:"d_following_holiday"`
	D_FirstDom         int       `csv:"d_first_dom"`
	D_LastDom          int       `csv:"d_last_dom"`
	D_SameDayLy        int       `csv:"d_same_day_ly"`
	D_SameDayLq        int       `csv:"d_same_day_lq"`
	D_CurrentDay       string    `csv:"d_current_day"`
	D_CurrentWeek      string    `csv:"d_current_week"`
	D_CurrentMonth     string    `csv:"d_current_month"`
	D_CurrentQuarter   string    `csv:"d_current_quarter"`
	D_CurrentYear      string    `csv:"d_current_year"`
}

// HouseholdDemographics defines a household demographic profile.
type HouseholdDemographics struct {
	HD_DemoSK      int64 `csv:"hd_demo_sk"`
	HD_IncomeBandSK int64 `csv:"hd_income_band_sk"`
	HD_BuyPotential string `csv:"hd_buy_potential"`
	HD_DepCount    int    `csv:"hd_dep_count"`
	HD_VehicleCount int    `csv:"hd_vehicle_count"`
}

// Item represents a unique product formulation.
type Item struct {
	I_ItemSK        int64   `csv:"i_item_sk"`
	I_ItemID        string  `csv:"i_item_id"`
	I_RecStartDate  string  `csv:"i_rec_start_date"`
	I_RecEndDate    string  `csv:"i_rec_end_date"`
	I_ItemDesc      string  `csv:"i_item_desc"`
	I_CurrentPrice  float64 `csv:"i_current_price"`
	I_WholesaleCost float64 `csv:"i_wholesale_cost"`
	I_BrandID       int     `csv:"i_brand_id"`
	I_Brand         string  `csv:"i_brand"`
	I_ClassID       int     `csv:"i_class_id"`
	I_Class         string  `csv:"i_class"`
	I_CategoryID    int     `csv:"i_category_id"`
	I_Category      string  `csv:"i_category"`
	I_ManufactID    int     `csv:"i_manufact_id"`
	I_Manufact      string  `csv:"i_manufact"`
	I_Size          string  `csv:"i_size"`
	I_Formulation   string  `csv:"i_formulation"`
	I_Color         string  `csv:"i_color"`
	I_Units         string  `csv:"i_units"`
	I_Container     string  `csv:"i_container"`
	I_ManagerID     int     `csv:"i_manager_id"`
	I_ProductName   string  `csv:"i_product_name"`
}

// IncomeBand represents details of an income range.
type IncomeBand struct {
	IB_IncomeBandSK   int64 `csv:"ib_income_band_sk"`
	IB_LowerBound     int   `csv:"ib_lower_bound"`
	IB_UpperBound     int   `csv:"ib_upper_bound"`
}

// Promotion represents details of a specific product promotion.
type Promotion struct {
	P_PromoSK         int64   `csv:"p_promo_sk"`
	P_PromoID         string  `csv:"p_promo_id"`
	P_StartDateSK     int64   `csv:"p_start_date_sk"`
	P_EndDateSK       int64   `csv:"p_end_date_sk"`
	P_ItemSK          int64   `csv:"p_item_sk"`
	P_Cost            float64 `csv:"p_cost"`
	P_ResponseTarget  int     `csv:"p_response_target"`
	P_PromoName       string  `csv:"p_promo_name"`
	P_ChannelDmail    string  `csv:"p_channel_dmail"`
	P_ChannelEmail    string  `csv:"p_channel_email"`
	P_ChannelCatalog  string  `csv:"p_channel_catalog"`
	P_ChannelTv       string  `csv:"p_channel_tv"`
	P_ChannelRadio    string  `csv:"p_channel_radio"`
	P_ChannelPress    string  `csv:"p_channel_press"`
	P_ChannelEvent    string  `csv:"p_channel_event"`
	P_ChannelDemo     string  `csv:"p_channel_demo"`
	P_ChannelDetails  string  `csv:"p_channel_details"`
	P_Purpose         string  `csv:"p_purpose"`
	P_DiscountActive  string  `csv:"p_discount_active"`
}

// Reason represents a reason why an item was returned.
type Reason struct {
	R_ReasonSK      int64  `csv:"r_reason_sk"`
	R_ReasonID      string `csv:"r_reason_id"`
	R_ReasonDesc    string `csv:"r_reason_desc"`
}

// ShipMode represents a shipping mode.
type ShipMode struct {
	SM_ShipModeSK    int64  `csv:"sm_ship_mode_sk"`
	SM_ShipModeID    string `csv:"sm_ship_mode_id"`
	SM_Type          string `csv:"sm_type"`
	SM_Code          string `csv:"sm_code"`
	SM_Carrier       string `csv:"sm_carrier"`
	SM_Contract      string `csv:"sm_contract"`
}

// TimeDim represents one second.
type TimeDim struct {
	T_TimeSK      int64  `csv:"t_time_sk"`
	T_TimeID      string `csv:"t_time_id"`
	T_Time        int    `csv:"t_time"`
	T_Hour        int    `csv:"t_hour"`
	T_Minute      int    `csv:"t_minute"`
	T_Second      int    `csv:"t_second"`
	T_AmPm        string `csv:"t_am_pm"`
	T_Shift       string `csv:"t_shift"`
	T_SubShift    string `csv:"t_sub_shift"`
	T_MealTime    string `csv:"t_meal_time"`
}
