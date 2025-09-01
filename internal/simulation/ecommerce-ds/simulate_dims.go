package ecommerceds

import (
	"fmt"
	"math/rand"
	"reflect"
	"time"

	ecommerceds "github.com/peekknuf/Gengo/internal/models/ecommerce-ds"
)

// GenerateStores generates a number of stores.
func GenerateStores(count int) []interface{} {
	stores := make([]interface{}, count)
	for i := 0; i < count; i++ {
		stores[i] = ecommerceds.Store{
			S_StoreSK:         int64(i + 1),
			S_StoreID:         fmt.Sprintf("store_%d", i+1),
			S_StoreName:       fmt.Sprintf("Store %d", i+1),
			S_NumberOfEmployees: 5 + rand.Intn(50),
			S_FloorSpace:      1000 + rand.Intn(4000),
			S_Hours:           "8am-10pm",
			S_Manager:         fmt.Sprintf("Manager %d", i+1),
			// ... other fields are placeholders
		}
	}
	return stores
}

// GenerateCallCenters generates a number of call centers.
func GenerateCallCenters(count int) []interface{} {
	centers := make([]interface{}, count)
	for i := 0; i < count; i++ {
		centers[i] = ecommerceds.CallCenter{
			CC_CallCenterSK: int64(i + 1),
			CC_CallCenterID: fmt.Sprintf("cc_%d", i+1),
			CC_Name:         fmt.Sprintf("Call Center %d", i+1),
			CC_Employees:    10 + rand.Intn(100),
			// ... other fields are placeholders
		}
	}
	return centers
}

// GenerateCatalogPages generates a number of catalog pages.
func GenerateCatalogPages(count int) []interface{} {
	pages := make([]interface{}, count)
	for i := 0; i < count; i++ {
		pages[i] = ecommerceds.CatalogPage{
			CP_CatalogPageSK:     int64(i + 1),
			CP_CatalogPageID:     fmt.Sprintf("cp_%d", i+1),
			CP_Department:        fmt.Sprintf("Dept %d", rand.Intn(10)+1),
			CP_CatalogNumber:     rand.Intn(5) + 1,
			CP_CatalogPageNumber: rand.Intn(100) + 1,
			// ... other fields are placeholders
		}
	}
	return pages
}

// GenerateWebSites generates a number of web sites.
func GenerateWebSites(count int) []interface{} {
	sites := make([]interface{}, count)
	for i := 0; i < count; i++ {
		sites[i] = ecommerceds.WebSite{
			Web_SiteSK:  int64(i + 1),
			Web_SiteID:  fmt.Sprintf("web_%d", i+1),
			Web_Name:    fmt.Sprintf("Web Site %d", i+1),
			Web_Manager: fmt.Sprintf("Web Manager %d", i+1),
			// ... other fields are placeholders
		}
	}
	return sites
}

// GenerateWebPages generates a number of web pages.
func GenerateWebPages(count int) []interface{} {
	pages := make([]interface{}, count)
	for i := 0; i < count; i++ {
		pages[i] = ecommerceds.WebPage{
			WP_WebPageSK: int64(i + 1),
			WP_WebPageID: fmt.Sprintf("wp_%d", i+1),
			WP_URL:       fmt.Sprintf("http://example.com/page%d", i+1),
			// ... other fields are placeholders
		}
	}
	return pages
}

// GenerateCustomers generates a number of customers.
func GenerateCustomers(count int, cdemoSKs, hdemoSKs, addrSKs []int64) []interface{} {
	customers := make([]interface{}, count)
	for i := 0; i < count; i++ {
		customers[i] = ecommerceds.Customer{
			C_CustomerSK:     int64(i + 1),
			C_CustomerID:     fmt.Sprintf("cust_%d", i+1),
			C_CurrentCDemoSK: cdemoSKs[rand.Intn(len(cdemoSKs))],
			C_CurrentHDemoSK: hdemoSKs[rand.Intn(len(hdemoSKs))],
			C_CurrentAddrSK:  addrSKs[rand.Intn(len(addrSKs))],
			C_FirstName:      fmt.Sprintf("First%d", i+1),
			C_LastName:       fmt.Sprintf("Last%d", i+1),
			C_EmailAddress:   fmt.Sprintf("cust%d@example.com", i+1),
			// ... other fields are placeholders
		}
	}
	return customers
}

// GenerateCustomerAddresses generates a number of customer addresses.
func GenerateCustomerAddresses(count int) []interface{} {
	addresses := make([]interface{}, count)
	for i := 0; i < count; i++ {
		addresses[i] = ecommerceds.CustomerAddress{
			CA_AddressSK: int64(i + 1),
			CA_AddressID: fmt.Sprintf("addr_%d", i+1),
			CA_City:      "Anytown",
			CA_State:     "Anystate",
			CA_Zip:       fmt.Sprintf("%05d", rand.Intn(99999)),
			CA_Country:   "USA",
			// ... other fields are placeholders
		}
	}
	return addresses
}

// GenerateCustomerDemographics generates a number of customer demographics.
func GenerateCustomerDemographics(count int) []interface{} {
	demos := make([]interface{}, count)
	for i := 0; i < count; i++ {
		demos[i] = ecommerceds.CustomerDemographics{
			CD_DemoSK:           int64(i + 1),
			CD_Gender:           "M", // Placeholder
			CD_MaritalStatus:    "Single", // Placeholder
			CD_EducationStatus:  "College", // Placeholder
			CD_PurchaseEstimate: 1000 + rand.Intn(9000),
			CD_CreditRating:     "Good", // Placeholder
			CD_DepCount:         rand.Intn(5),
			CD_DepEmployedCount: rand.Intn(5),
			CD_DepCollegeCount:  rand.Intn(5),
		}
	}
	return demos
}

// GenerateHouseholdDemographics generates a number of household demographics.
func GenerateHouseholdDemographics(count int, incomeBandSKs []int64) []interface{} {
	demos := make([]interface{}, count)
	for i := 0; i < count; i++ {
		demos[i] = ecommerceds.HouseholdDemographics{
			HD_DemoSK:       int64(i + 1),
			HD_IncomeBandSK: incomeBandSKs[rand.Intn(len(incomeBandSKs))],
			HD_BuyPotential: ">10000", // Placeholder
			HD_DepCount:     rand.Intn(5),
			HD_VehicleCount: rand.Intn(4),
		}
	}
	return demos
}

// GenerateItems generates a number of items.
func GenerateItems(count int) []interface{} {
	items := make([]interface{}, count)
	for i := 0; i < count; i++ {
		items[i] = ecommerceds.Item{
			I_ItemSK:        int64(i + 1),
			I_ItemID:        fmt.Sprintf("item_%d", i+1),
			I_ItemDesc:      fmt.Sprintf("Item description %d", i+1),
			I_CurrentPrice:  10.0 + rand.Float64()*1000,
			I_WholesaleCost: 5.0 + rand.Float64()*500,
			I_BrandID:       rand.Intn(100) + 1,
			I_Brand:         fmt.Sprintf("Brand %d", rand.Intn(100)+1),
			I_ClassID:       rand.Intn(20) + 1,
			I_Class:         fmt.Sprintf("Class %d", rand.Intn(20)+1),
			I_CategoryID:    rand.Intn(10) + 1,
			I_Category:      fmt.Sprintf("Category %d", rand.Intn(10)+1),
			I_ManufactID:    rand.Intn(50) + 1,
			I_Manufact:      fmt.Sprintf("Manufacturer %d", rand.Intn(50)+1),
			I_Size:          "Medium", // Placeholder
			I_Formulation:   "Formulation", // Placeholder
			I_Color:         "Red", // Placeholder
			I_Units:         "Each", // Placeholder
			I_Container:     "Box", // Placeholder
			I_ManagerID:     rand.Intn(10) + 1,
			I_ProductName:   fmt.Sprintf("Product %d", i+1),
		}
	}
	return items
}

// GenerateReasons generates a fixed set of return reasons.
func GenerateReasons(count int) []interface{} {
	reasons := make([]interface{}, count)
	for i := 0; i < count; i++ {
		reasons[i] = ecommerceds.Reason{
			R_ReasonSK:   int64(i + 1),
			R_ReasonID:   fmt.Sprintf("reason_%d", i+1),
			R_ReasonDesc: fmt.Sprintf("Reason description %d", i+1),
		}
	}
	return reasons
}

// GenerateShipModes generates a fixed set of shipping modes.
func GenerateShipModes(count int) []interface{} {
	shipModes := make([]interface{}, count)
	carriers := []string{"UPS", "FedEx", "USPS", "DHL"}
	for i := 0; i < count; i++ {
		shipModes[i] = ecommerceds.ShipMode{
			SM_ShipModeSK:  int64(i + 1),
			SM_ShipModeID:  fmt.Sprintf("sm_%d", i+1),
			SM_Type:        fmt.Sprintf("Type %d", i+1),
			SM_Code:        fmt.Sprintf("Code %d", i+1),
			SM_Carrier:     carriers[rand.Intn(len(carriers))],
			SM_Contract:    fmt.Sprintf("Contract %d", i+1),
		}
	}
	return shipModes
}

// GenerateIncomeBands generates a fixed set of income bands.
func GenerateIncomeBands(count int) []interface{} {
	incomeBands := make([]interface{}, count)
	for i := 0; i < count; i++ {
		lowerBound := 30000 + i*10000
		upperBound := lowerBound + 9999
		incomeBands[i] = ecommerceds.IncomeBand{
			IB_IncomeBandSK: int64(i + 1),
			IB_LowerBound:   lowerBound,
			IB_UpperBound:   upperBound,
		}
	}
	return incomeBands
}

// GeneratePromotions generates a number of promotions.
func GeneratePromotions(count int, itemSKs []int64) []interface{} {
	promotions := make([]interface{}, count)
	for i := 0; i < count; i++ {
		promotions[i] = ecommerceds.Promotion{
			P_PromoSK:        int64(i + 1),
			P_PromoID:        fmt.Sprintf("promo_%d", i+1),
			P_StartDateSK:    int64(2451545 + i*2), // Placeholder
			P_EndDateSK:      int64(2451545 + i*2 + 30),
			P_ItemSK:         itemSKs[rand.Intn(len(itemSKs))],
			P_Cost:           rand.Float64() * 10000,
			P_ResponseTarget: rand.Intn(2),
			P_PromoName:      fmt.Sprintf("Promotion #%d", i+1),
			P_ChannelDmail:   "N",
			P_ChannelEmail:   "Y",
			P_ChannelCatalog: "N",
			P_ChannelTv:      "Y",
			P_ChannelRadio:   "N",
			P_ChannelPress:   "N",
			P_ChannelEvent:   "N",
			P_ChannelDemo:    "N",
			P_ChannelDetails: "",
			P_Purpose:        "general promotion",
			P_DiscountActive: "Y",
		}
	}
	return promotions
}

// GenerateWarehouses generates a number of warehouses.
func GenerateWarehouses(count int) []interface{} {
	warehouses := make([]interface{}, count)
	for i := 0; i < count; i++ {
		warehouses[i] = ecommerceds.Warehouse{
			W_WarehouseSK:   int64(i + 1),
			W_WarehouseID:   fmt.Sprintf("wh_%d", i+1),
			W_WarehouseName: fmt.Sprintf("Warehouse %d", i+1),
			W_WarehouseSqFt: 10000 + rand.Intn(90000),
			W_StreetNumber:  fmt.Sprintf("%d", rand.Intn(1000)),
			W_StreetName:    "Main St",
			W_StreetType:    "Street",
			W_SuiteNumber:   fmt.Sprintf("Suite %d", rand.Intn(100)),
			W_City:          "Anytown",
			W_County:        "Anycounty",
			W_State:         "Anystate",
			W_Zip:           fmt.Sprintf("%05d", rand.Intn(99999)),
			W_Country:       "USA",
			W_GmtOffset:     -5.0,
		}
	}
	return warehouses
}

// GenerateTimeDim generates time dimension data for a 24-hour period.
func GenerateTimeDim() []interface{} {
	timeDims := make([]interface{}, 24*60*60)
	for h := 0; h < 24; h++ {
		for m := 0; m < 60; m++ {
			for s := 0; s < 60; s++ {
				index := h*3600 + m*60 + s
				timeDims[index] = ecommerceds.TimeDim{
					T_TimeSK:   int64(index),
					T_TimeID:   fmt.Sprintf("%02d:%02d:%02d", h, m, s),
					T_Time:     index,
					T_Hour:     h,
					T_Minute:   m,
					T_Second:   s,
					T_AmPm:     "AM", // Placeholder
					T_Shift:    "Day",  // Placeholder
					T_SubShift: "Sub-shift 1", // Placeholder
					T_MealTime: "Lunch", // Placeholder
				}
			}
		}
	}
	return timeDims
}

// GenerateDateDim generates date dimension data for a range of years.
func GenerateDateDim(startYear, endYear int) []interface{} {
	var dateDims []interface{}
	startDate := time.Date(startYear, 1, 1, 0, 0, 0, 0, time.UTC)
	endDate := time.Date(endYear, 12, 31, 0, 0, 0, 0, time.UTC)

	for d := startDate; !d.After(endDate); d = d.AddDate(0, 0, 1) {
		dateDims = append(dateDims, ecommerceds.DateDim{
			D_DateSK:         int64(d.Unix() / (24 * 60 * 60)), // Julian date
			D_DateID:         d.Format("2006-01-02"),
			D_Date:           d,
			D_Year:           d.Year(),
			D_Moy:            int(d.Month()),
			D_Dom:            d.Day(),
			D_Dow:            int(d.Weekday()),
			D_DayName:        d.Weekday().String(),
			// ... other fields are placeholders for now
		})
	}
	return dateDims
}

func getSKsFromChan(ch <-chan interface{}) []int64 {
	var sks []int64
	for v := range ch {
		sks = append(sks, reflect.ValueOf(v).Field(0).Int())
	}
	return sks
}
