package ecommerceds

import (
	"fmt"
	"math/rand"
	"reflect"
	"strings"
	"time"

	ecommerceds "github.com/peekknuf/Gengo/internal/models/ecommerce-ds"
)

// GenerateStores generates a number of stores.
func GenerateStores(count int) []interface{} {
	stores := make([]interface{}, count)
	cities := []string{"New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Philadelphia", "San Antonio", "San Diego", "Dallas", "San Jose"}
	states := []string{"NY", "CA", "IL", "TX", "AZ", "PA", "TX", "CA", "TX", "CA"}
	counties := []string{"Manhattan", "Los Angeles County", "Cook County", "Harris County", "Maricopa County", "Philadelphia County", "Bexar County", "San Diego County", "Dallas County", "Santa Clara County"}
	
	for i := 0; i < count; i++ {
		cityIdx := i % len(cities)
		stores[i] = ecommerceds.Store{
			S_StoreSK:        int64(i + 1),
			S_StoreID:        fmt.Sprintf("store_%d", i+1),
			S_StoreName:      fmt.Sprintf("Store %d", i+1),
			S_StoreNumber:    1000 + i,
			S_StreetNumber:   fmt.Sprintf("%d", rand.Intn(9999)+1),
			S_StreetName:     fmt.Sprintf("Main St"),
			S_StreetType:     "Street",
			S_SuiteNumber:    fmt.Sprintf("Suite %d", rand.Intn(100)+1),
			S_City:           cities[cityIdx],
			S_County:         counties[cityIdx],
			S_State:          states[cityIdx],
			S_Zip:            fmt.Sprintf("%05d", rand.Intn(99999)+1),
			S_Country:        "USA",
			S_GmtOffset:      -5.0,
			S_TaxPrecentage:  0.08 + rand.Float64()*0.05,
			S_FloorSpace:     1000 + rand.Intn(4000),
			S_Hours:          "8am-10pm",
			S_Manager:        fmt.Sprintf("Manager %d", i+1),
			S_MarketID:       rand.Intn(10) + 1,
			S_GeographyClass: fmt.Sprintf("Class %d", rand.Intn(5)+1),
			S_MarketDesc:     fmt.Sprintf("Market %d", rand.Intn(5)+1),
			S_MarketManager:  fmt.Sprintf("Market Manager %d", i+1),
			S_DivisionID:     rand.Intn(5) + 1,
			S_DivisionName:   fmt.Sprintf("Division %d", rand.Intn(5)+1),
			S_CompanyID:      rand.Intn(3) + 1,
			S_CompanyName:    fmt.Sprintf("Company %d", rand.Intn(3)+1),
		}
	}
	return stores
}

// GenerateCallCenters generates a number of call centers.
func GenerateCallCenters(count int) []interface{} {
	centers := make([]interface{}, count)
	
	for i := 0; i < count; i++ {
		employees := 50 + rand.Intn(200)
		hours := []string{"24/7", "8am-8pm", "9am-9pm", "7am-11pm", "6am-10pm"}
		hourIdx := rand.Intn(len(hours))
		
		centers[i] = ecommerceds.CallCenter{
			CC_CallCenterSK:     int64(i + 1),
			CC_CallCenterID:     fmt.Sprintf("cc_%d", i+1),
			CC_Name:             fmt.Sprintf("Call Center %d", i+1),
			CC_Class:            fmt.Sprintf("Class %d", rand.Intn(5)+1),
			CC_Employees:        employees,
			CC_SqFt:             5000 + rand.Intn(15000),
			CC_Hours:            hours[hourIdx],
			CC_Manager:          fmt.Sprintf("Manager %d", i+1),
			CC_MktID:            rand.Intn(10) + 1,
			CC_MktClass:         fmt.Sprintf("Market Class %d", rand.Intn(3)+1),
			CC_MktDesc:          fmt.Sprintf("Market Description %d", rand.Intn(5)+1),
			CC_MarketManager:    fmt.Sprintf("Market Manager %d", i+1),
			CC_Division:         rand.Intn(5) + 1,
			CC_DivisionName:     fmt.Sprintf("Division %d", rand.Intn(5)+1),
			CC_Company:          rand.Intn(3) + 1,
			CC_CompanyName:      fmt.Sprintf("Company %d", rand.Intn(3)+1),
		}
	}
	return centers
}

// GenerateCatalogPages generates a number of catalog pages.
func GenerateCatalogPages(count int) []interface{} {
	pages := make([]interface{}, count)
	departments := []string{"Electronics", "Clothing", "Home", "Sports", "Books", "Toys", "Beauty", "Automotive", "Garden", "Health"}
	startYear := 2020
	endYear := 2025
	
	for i := 0; i < count; i++ {
		deptIdx := i % len(departments)
		catalogNum := rand.Intn(5) + 1
		pageNum := rand.Intn(100) + 1
		pageType := []string{"Regular", "Sale", "Clearance", "Featured", "New"}
		typeIdx := rand.Intn(len(pageType))
		
		startDate := time.Date(startYear+rand.Intn(endYear-startYear), time.Month(rand.Intn(12)+1), 1, 0, 0, 0, 0, time.UTC)
		endDate := startDate.AddDate(0, 3, 0)
		
		pages[i] = ecommerceds.CatalogPage{
			CP_CatalogPageSK:     int64(i + 1),
			CP_CatalogPageID:     fmt.Sprintf("cp_%d_%d_%d", catalogNum, pageNum, i+1),
			CP_StartDateSK:       startDate.Unix() / (24 * 60 * 60),
			CP_EndDateSK:         endDate.Unix() / (24 * 60 * 60),
			CP_Department:        fmt.Sprintf("%s", departments[deptIdx]),
			CP_CatalogNumber:     catalogNum,
			CP_CatalogPageNumber: pageNum,
			CP_Description:       fmt.Sprintf("%s Page %d", departments[deptIdx], pageNum),
			CP_Type:              pageType[typeIdx],
		}
	}
	return pages
}

// GenerateWebSites generates a number of web sites.
func GenerateWebSites(count int) []interface{} {
	sites := make([]interface{}, count)
	
	for i := 0; i < count; i++ {
		managerID := rand.Intn(20) + 1
		
		sites[i] = ecommerceds.WebSite{
			Web_SiteSK:        int64(i + 1),
			Web_SiteID:        fmt.Sprintf("web_%d", i+1),
			Web_Name:          fmt.Sprintf("Web Site %d", i+1),
			Web_Manager:       fmt.Sprintf("Web Manager %d", managerID),
			Web_CompanyID:     rand.Intn(10) + 1,
			Web_CompanyName:   fmt.Sprintf("Web Company %d", rand.Intn(10)+1),
		}
	}
	return sites
}

// GenerateWebPages generates a number of web pages.
func GenerateWebPages(count int) []interface{} {
	pages := make([]interface{}, count)
	pageTypes := []string{"Product", "Category", "Search", "Checkout", "Account", "Home", "About", "Contact"}
	
	for i := 0; i < count; i++ {
		typeIdx := rand.Intn(len(pageTypes))
		
		pages[i] = ecommerceds.WebPage{
			WP_WebPageSK:      int64(i + 1),
			WP_WebPageID:      fmt.Sprintf("wp_%d", i+1),
			WP_CreationDateSK: int64(2451545 + i), // Placeholder date
			WP_AccessDateSK:   int64(2451545 + i + 1), // Placeholder date
			WP_AutogenFlag:    "N",
			WP_CustomerSK:     int64(rand.Intn(1000) + 1), // Random customer
			WP_URL:            fmt.Sprintf("http://example.com/page%d", i+1),
			WP_Type:           pageTypes[typeIdx],
		}
	}
	return pages
}

// GenerateCustomers generates a number of customers.
func GenerateCustomers(count int, cdemoSKs, hdemoSKs, addrSKs []int64) []interface{} {
	customers := make([]interface{}, count)
	firstNames := []string{"John", "Jane", "Michael", "Sarah", "David", "Emily", "Robert", "Lisa", "James", "Jennifer"}
	lastNames := []string{"Smith", "Johnson", "Williams", "Brown", "Jones", "Miller", "Davis", "Garcia", "Rodriguez", "Wilson"}
	
	for i := 0; i < count; i++ {
		if len(cdemoSKs) == 0 || len(hdemoSKs) == 0 || len(addrSKs) == 0 {
			continue // Skip if required dependencies are not available
		}
		
		firstName := firstNames[rand.Intn(len(firstNames))]
		lastName := lastNames[rand.Intn(len(lastNames))]
		birthDate := time.Date(1950+rand.Intn(50), time.Month(rand.Intn(12)+1), rand.Intn(28)+1, 0, 0, 0, 0, time.UTC)
		
		customers[i] = ecommerceds.Customer{
			C_CustomerSK:          int64(i + 1),
			C_CustomerID:          fmt.Sprintf("cust_%d", i+1),
			C_CurrentCDemoSK:      cdemoSKs[rand.Intn(len(cdemoSKs))],
			C_CurrentHDemoSK:      hdemoSKs[rand.Intn(len(hdemoSKs))],
			C_CurrentAddrSK:       addrSKs[rand.Intn(len(addrSKs))],
			C_FirstShiptoDateSK:   int64(2451545 + i), // Placeholder date
			C_FirstSalesDateSK:    int64(2451545 + i + 1), // Placeholder date
			C_Salutation:          []string{"Mr", "Ms", "Mrs", "Dr"}[rand.Intn(4)],
			C_FirstName:           firstName,
			C_LastName:            lastName,
			C_PreferredCustFlag:   []string{"Y", "N"}[rand.Intn(2)],
			C_BirthDay:            birthDate.Day(),
			C_BirthMonth:          int(birthDate.Month()),
			C_BirthYear:           birthDate.Year(),
			C_BirthCountry:        "USA",
			C_Login:               fmt.Sprintf("%s%s%d", strings.ToLower(firstName)[:3], strings.ToLower(lastName)[:3], i+1),
			C_EmailAddress:        fmt.Sprintf("%s.%s%d@example.com", strings.ToLower(firstName), strings.ToLower(lastName), i+1),
			C_LastReviewDateSK:    int64(2451545 + i + 2), // Placeholder date
		}
	}
	return customers
}

// GenerateCustomerAddresses generates a number of customer addresses.
func GenerateCustomerAddresses(count int) []interface{} {
	addresses := make([]interface{}, count)
	cities := []string{"New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Philadelphia", "San Antonio", "San Diego", "Dallas", "San Jose"}
	states := []string{"NY", "CA", "IL", "TX", "AZ", "PA", "TX", "CA", "TX", "CA"}
	streets := []string{"Main St", "Oak Ave", "Pine Rd", "Elm Blvd", "Maple Dr", "Cedar Ln", "Birch Way", "Walnut St", "Ash Ave", "Cherry Rd"}
	
	for i := 0; i < count; i++ {
		cityIdx := i % len(cities)
		fromDate := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
		
		addresses[i] = ecommerceds.CustomerAddress{
			CA_AddressSK:     int64(i + 1),
			CA_AddressID:     fmt.Sprintf("addr_%d", i+1),
			CA_AddressDateSK: fromDate.Unix() / (24 * 60 * 60),
			CA_StreetNumber:  fmt.Sprintf("%d", rand.Intn(9999)+1),
			CA_StreetName:    streets[rand.Intn(len(streets))],
			CA_StreetType:    "St",
			CA_SuiteNumber:   fmt.Sprintf("Apt %d", rand.Intn(500)+1),
			CA_City:          cities[cityIdx],
			CA_County:        fmt.Sprintf("%s County", cities[cityIdx]),
			CA_State:         states[cityIdx],
			CA_Zip:           fmt.Sprintf("%05d", rand.Intn(99999)+1),
			CA_Country:       "USA",
			CA_GmtOffset:     -5.0,
			CA_LocationType:  []string{"Residential", "Business", "PO Box", "Military"}[rand.Intn(4)],
		}
	}
	return addresses
}

// GenerateCustomerDemographics generates a number of customer demographics.
func GenerateCustomerDemographics(count int) []interface{} {
	demos := make([]interface{}, count)
	genders := []string{"M", "F"}
	maritalStatuses := []string{"Single", "Married", "Divorced", "Widowed"}
	educationLevels := []string{"High School", "College", "Graduate", "Post-Graduate"}
	creditRatings := []string{"Poor", "Fair", "Good", "Excellent"}
	
	for i := 0; i < count; i++ {
		gender := genders[rand.Intn(len(genders))]
		maritalStatus := maritalStatuses[rand.Intn(len(maritalStatuses))]
		education := educationLevels[rand.Intn(len(educationLevels))]
		creditRating := creditRatings[rand.Intn(len(creditRatings))]
		
		demos[i] = ecommerceds.CustomerDemographics{
			CD_DemoSK:              int64(i + 1),
			CD_Gender:              gender,
			CD_MaritalStatus:       maritalStatus,
			CD_EducationStatus:     education,
			CD_PurchaseEstimate:    1000 + rand.Intn(9000),
			CD_CreditRating:        creditRating,
			CD_DepCount:            rand.Intn(5),
			CD_DepEmployedCount:    rand.Intn(5),
			CD_DepCollegeCount:     rand.Intn(5),
			CD_HouseholdSize:       1 + rand.Intn(8),
			CD_AverageYearlyIncome: int64(20000 + rand.Intn(150000)),
			CD_CustomerSegment:     []string{"Basic", "Standard", "Premium", "VIP"}[rand.Intn(4)],
		}
	}
	return demos
}

// GenerateHouseholdDemographics generates a number of household demographics.
func GenerateHouseholdDemographics(count int, incomeBandSKs []int64) []interface{} {
	demos := make([]interface{}, count)
	buyPotentials := []string{"<1000", "1000-5000", "5000-10000", "10000-20000", ">20000"}
	
	for i := 0; i < count; i++ {
		if len(incomeBandSKs) == 0 {
			continue // Skip if no income bands available
		}
		
		demos[i] = ecommerceds.HouseholdDemographics{
			HD_DemoSK:       int64(i + 1),
			HD_IncomeBandSK: incomeBandSKs[rand.Intn(len(incomeBandSKs))],
			HD_BuyPotential: buyPotentials[rand.Intn(len(buyPotentials))],
			HD_DepCount:     1 + rand.Intn(8),
			HD_VehicleCount: rand.Intn(5),
		}
	}
	return demos
}

// GenerateItems generates a number of items.
func GenerateItems(count int) []interface{} {
	items := make([]interface{}, count)
	categories := []string{"Electronics", "Clothing", "Home", "Sports", "Books", "Toys", "Beauty", "Automotive", "Garden", "Health"}
	brands := []string{"TechCorp", "FashionHub", "HomeStyle", "SportPro", "BookWorld", "ToyLand", "BeautyPlus", "AutoMax", "GardenGreen", "HealthFirst"}
	sizes := []string{"XS", "S", "M", "L", "XL", "XXL"}
	colors := []string{"Red", "Blue", "Green", "Black", "White", "Yellow", "Purple", "Orange", "Pink", "Gray"}
	units := []string{"Each", "Dozen", "Case", "Pack", "Set", "Box", "Bag", "Can", "Bottle", "Tube"}
	containers := []string{"Box", "Bag", "Can", "Bottle", "Tube", "Jar", "Pouch", "Case", "Pack", "Set"}
	
	for i := 0; i < count; i++ {
		category := categories[rand.Intn(len(categories))]
		brand := brands[rand.Intn(len(brands))]
		wholesaleCost := 5.0 + rand.Float64()*500
		retailPrice := wholesaleCost * (1.2 + rand.Float64()*0.8)
		
		items[i] = ecommerceds.Item{
			I_ItemSK:        int64(i + 1),
			I_ItemID:        fmt.Sprintf("item_%d", i+1),
			I_ItemDesc:      fmt.Sprintf("%s %s %d", brand, category, i+1),
			I_CurrentPrice:  retailPrice,
			I_WholesaleCost: wholesaleCost,
			I_BrandID:       rand.Intn(100) + 1,
			I_Brand:         brand,
			I_ClassID:       rand.Intn(20) + 1,
			I_Class:         fmt.Sprintf("Class %d", rand.Intn(20)+1),
			I_CategoryID:    rand.Intn(10) + 1,
			I_Category:      category,
			I_ManufactID:    rand.Intn(50) + 1,
			I_Manufact:      fmt.Sprintf("Manufacturer %d", rand.Intn(50)+1),
			I_Size:          sizes[rand.Intn(len(sizes))],
			I_Formulation:   fmt.Sprintf("Formulation %d", rand.Intn(10)+1),
			I_Color:         colors[rand.Intn(len(colors))],
			I_Units:         units[rand.Intn(len(units))],
			I_Container:     containers[rand.Intn(len(containers))],
			I_ManagerID:     rand.Intn(10) + 1,
			I_ProductName:   fmt.Sprintf("%s %s", brand, category),
		}
	}
	return items
}

// GenerateReasons generates a fixed set of return reasons.
func GenerateReasons(count int) []interface{} {
	reasons := make([]interface{}, count)
	reasonDescs := []string{
		"Defective Product",
		"Wrong Item Shipped",
		"Damaged in Transit",
		"Not as Described",
		"Size/Color Mismatch",
		"Changed Mind",
		"Found Better Price",
		"No Longer Needed",
		"Gift Return",
		"Late Delivery",
		"Quality Issues",
		"Fits Poorly",
		"Uncomfortable",
		"Difficult to Use",
		"Missing Parts",
		"Allergic Reaction",
		"Duplicate Purchase",
		"Shipping Error",
		"Customer Dissatisfaction",
		"Other",
	}
	
	for i := 0; i < count; i++ {
		if i < len(reasonDescs) {
			reasons[i] = ecommerceds.Reason{
				R_ReasonSK:   int64(i + 1),
				R_ReasonID:   fmt.Sprintf("reason_%d", i+1),
				R_ReasonDesc: reasonDescs[i],
			}
		} else {
			// If we need more reasons than we have predefined
			reasons[i] = ecommerceds.Reason{
				R_ReasonSK:   int64(i + 1),
				R_ReasonID:   fmt.Sprintf("reason_%d", i+1),
				R_ReasonDesc: fmt.Sprintf("Return Reason %d", i+1),
			}
		}
	}
	return reasons
}

// GenerateShipModes generates a fixed set of shipping modes.
func GenerateShipModes(count int) []interface{} {
	shipModes := make([]interface{}, count)
	shipTypes := []string{"Ground", "Air", "Sea", "Rail", "Express", "Overnight", "Standard", "Economy"}
	carriers := []string{"UPS", "FedEx", "USPS", "DHL", "Amazon Logistics", "OnTrac", "LaserShip", "Regional Carrier"}
	
	for i := 0; i < count; i++ {
		shipType := shipTypes[i%len(shipTypes)]
		carrier := carriers[rand.Intn(len(carriers))]
		
		shipModes[i] = ecommerceds.ShipMode{
			SM_ShipModeSK:  int64(i + 1),
			SM_ShipModeID:  fmt.Sprintf("sm_%d", i+1),
			SM_Type:        shipType,
			SM_Code:        fmt.Sprintf("CODE%d", i+1),
			SM_Carrier:     carrier,
			SM_Contract:    fmt.Sprintf("CONTRACT%d", i+1),
		}
	}
	return shipModes
}

// GenerateIncomeBands generates a fixed set of income bands.
func GenerateIncomeBands(count int) []interface{} {
	incomeBands := make([]interface{}, count)
	
	for i := 0; i < count; i++ {
		lowerBound := 20000 + i*25000
		upperBound := lowerBound + 24999
		
		incomeBands[i] = ecommerceds.IncomeBand{
			IB_IncomeBandSK:   int64(i + 1),
			IB_LowerBound:     lowerBound,
			IB_UpperBound:     upperBound,
		}
	}
	return incomeBands
}

// GeneratePromotions generates a number of promotions.
func GeneratePromotions(count int, itemSKs []int64) []interface{} {
	promotions := make([]interface{}, count)
	promoTypes := []string{"Discount", "Coupon", "Bundle", "BOGO", "Clearance", "Seasonal", "New Customer", "Loyalty", "Referral", "Flash Sale"}
	
	for i := 0; i < count; i++ {
		if len(itemSKs) == 0 {
			continue // Skip if no items available
		}
		
		startDate := time.Date(2020+rand.Intn(5), time.Month(rand.Intn(12)+1), 1, 0, 0, 0, 0, time.UTC)
		endDate := startDate.AddDate(0, 0, rand.Intn(90)+30)
		
		promotions[i] = ecommerceds.Promotion{
			P_PromoSK:           int64(i + 1),
			P_PromoID:           fmt.Sprintf("promo_%d", i+1),
			P_StartDateSK:       startDate.Unix() / (24 * 60 * 60),
			P_EndDateSK:         endDate.Unix() / (24 * 60 * 60),
			P_ItemSK:            itemSKs[rand.Intn(len(itemSKs))],
			P_Cost:              rand.Float64() * 10000,
			P_TargetMarketClass: []string{"Mass", "Upscale", "Luxury", "Budget"}[rand.Intn(4)],
			P_PromoName:         fmt.Sprintf("%s Promotion %d", promoTypes[rand.Intn(len(promoTypes))], i+1),
			P_ChannelDmail:      []string{"Y", "N"}[rand.Intn(2)],
			P_ChannelEmail:      []string{"Y", "N"}[rand.Intn(2)],
			P_ChannelCatalog:    []string{"Y", "N"}[rand.Intn(2)],
			P_ChannelTv:         []string{"Y", "N"}[rand.Intn(2)],
			P_ChannelRadio:      []string{"Y", "N"}[rand.Intn(2)],
			P_ChannelPress:      []string{"Y", "N"}[rand.Intn(2)],
			P_ChannelEvent:      []string{"Y", "N"}[rand.Intn(2)],
			P_ChannelDemo:       []string{"Y", "N"}[rand.Intn(2)],
			P_Purpose:           fmt.Sprintf("%s campaign", promoTypes[rand.Intn(len(promoTypes))]),
			P_DiscountActive:    "Y",
		}
	}
	return promotions
}

// GenerateWarehouses generates a number of warehouses.
func GenerateWarehouses(count int) []interface{} {
	warehouses := make([]interface{}, count)
	states := []string{"CA", "TX", "FL", "NY", "IL", "PA", "OH", "GA", "NC", "MI"}
	warehouseTypes := []string{"Distribution Center", "Fulfillment Center", "Cross-Dock", "Cold Storage", "Bulk Storage", "Retail Warehouse"}
	
	for i := 0; i < count; i++ {
		state := states[rand.Intn(len(states))]
		warehouseType := warehouseTypes[rand.Intn(len(warehouseTypes))]
		
		warehouses[i] = ecommerceds.Warehouse{
			W_WarehouseSK:      int64(i + 1),
			W_WarehouseID:      fmt.Sprintf("wh_%d", i+1),
			W_WarehouseName:    fmt.Sprintf("%s %d", warehouseType, i+1),
			W_WarehouseSqFt:    50000 + rand.Intn(500000),
			W_StreetNumber:     fmt.Sprintf("%d", rand.Intn(9999)+1),
			W_StreetName:       "Industrial Blvd",
			W_StreetType:       "Street",
			W_SuiteNumber:      fmt.Sprintf("Suite %d", rand.Intn(100)+1),
			W_City:             fmt.Sprintf("Industrial City %d", i+1),
			W_County:           fmt.Sprintf("Manufacturing County %d", i+1),
			W_State:            state,
			W_Zip:              fmt.Sprintf("%05d", rand.Intn(99999)+1),
			W_Country:          "USA",
			W_GmtOffset:        -5.0,
			W_TaxPercentage:    0.08 + rand.Float64()*0.05,
		}
	}
	return warehouses
}

// GenerateTimeDim generates time dimension data for a 24-hour period.
func GenerateTimeDim() []interface{} {
	timeDims := make([]interface{}, 24*60*60)
	
	for h := 0; h < 24; h++ {
		var amPm string
		if h < 12 {
			amPm = "AM"
		} else {
			amPm = "PM"
		}
		
		var shift string
		switch {
		case h >= 6 && h < 14:
			shift = "Morning"
		case h >= 14 && h < 22:
			shift = "Evening"
		default:
			shift = "Night"
		}
		
		var subShift string
		switch {
		case h >= 6 && h < 10:
			subShift = "Early Morning"
		case h >= 10 && h < 14:
			subShift = "Late Morning"
		case h >= 14 && h < 18:
			subShift = "Afternoon"
		case h >= 18 && h < 22:
			subShift = "Evening"
		default:
			subShift = "Night"
		}
		
		var mealTime string
		switch h {
		case 7, 8:
			mealTime = "Breakfast"
		case 12, 13:
			mealTime = "Lunch"
		case 18, 19:
			mealTime = "Dinner"
		default:
			mealTime = "Regular"
		}
		
		for m := 0; m < 60; m++ {
			for s := 0; s < 60; s++ {
				index := h*3600 + m*60 + s
				
				timeDims[index] = ecommerceds.TimeDim{
					T_TimeSK:        int64(index),
					T_TimeID:        fmt.Sprintf("%02d:%02d:%02d", h, m, s),
					T_Time:          index,
					T_Hour:          h,
					T_Minute:        m,
					T_Second:        s,
					T_TimezoneID:    1, // Default timezone
					T_AmPm:          amPm,
					T_Shift:         shift,
					T_SubShift:      subShift,
					T_MealTime:      mealTime,
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
		year := d.Year()
		month := int(d.Month())
		day := d.Day()
		dow := int(d.Weekday())
		isWeekend := dow == 0 || dow == 6
		
		var quarter int
		switch {
		case month <= 3:
			quarter = 1
		case month <= 6:
			quarter = 2
		case month <= 9:
			quarter = 3
		default:
			quarter = 4
		}
		
		dateDims = append(dateDims, ecommerceds.DateDim{
			D_DateSK:           int64(d.Unix() / (24 * 60 * 60)), // Julian date
			D_DateID:           d.Format("2006-01-02"),
			D_Date:             d,
			D_MonthSeq:         year*12 + month - 1,
			D_WeekSeq:          year*52 + d.YearDay()/7,
			D_QuarterSeq:       year*4 + quarter - 1,
			D_Year:             year,
			D_Dow:              dow,
			D_Moy:              month,
			D_Dom:              day,
			D_Qoy:              (month-1)%3 + 1,
			D_FyYear:           year,
			D_FyQuarterSeq:     year*4 + quarter - 1,
			D_FyWeekSeq:        year*52 + d.YearDay()/7,
			D_DayName:          d.Weekday().String(),
			D_QuarterName:      fmt.Sprintf("Q%d", quarter),
			D_Holiday:          "N",
			D_Weekend:          map[bool]string{true: "Y", false: "N"}[isWeekend],
			D_FollowingHoliday: "N",
			D_FirstDom:         1,
			D_LastDom:          daysInMonth(month, year),
			D_SameDayLy:        0,
			D_SameDayLq:        0,
			D_CurrentDay:       "N",
			D_CurrentWeek:      "N",
			D_CurrentMonth:     "N",
			D_CurrentQuarter:   "N",
			D_CurrentYear:      "N",
		})
	}
	return dateDims
}

// Helper functions for date dimension
func isLeapYear(year int) bool {
	if year%4 != 0 {
		return false
	} else if year%100 != 0 {
		return true
	} else {
		return year%400 == 0
	}
}

func daysInMonth(month, year int) int {
	switch month {
	case 1, 3, 5, 7, 8, 10, 12:
		return 31
	case 4, 6, 9, 11:
		return 30
	case 2:
		if isLeapYear(year) {
			return 29
		}
		return 28
	default:
		return 30
	}
}

func dayInQuarter(month, day, year int) int {
	quarterStartMonth := ((month-1)/3)*3 + 1
	quarterStartDay := 1
	
	totalDays := 0
	for m := quarterStartMonth; m < month; m++ {
		totalDays += daysInMonth(m, year)
	}
	totalDays += day - quarterStartDay
	
	return totalDays
}

func getSKsFromChan(ch <-chan interface{}) []int64 {
	var sks []int64
	for v := range ch {
		sks = append(sks, reflect.ValueOf(v).Field(0).Int())
	}
	return sks
}
