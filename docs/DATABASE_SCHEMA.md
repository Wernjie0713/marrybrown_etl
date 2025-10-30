# Database Schema Reference

**Project**: Marrybrown Cloud Data Warehouse  
**Schema Type**: Star Schema (Dimensional Model)  
**Purpose**: Optimized for analytical queries and business intelligence

---

## 📊 Star Schema Overview

The data warehouse uses a **star schema** design with one central fact table surrounded by dimension tables. This design optimizes query performance for analytics while keeping the model simple and intuitive.

```
                  ┌─────────────────┐
                  │   dim_date      │
                  │ (Calendar)      │
                  └────────┬────────┘
                           │
    ┌──────────────┐       │         ┌──────────────┐
    │ dim_products │       │         │ dim_customers│
    │ (Menu Items) │       │         │ (Loyalty)    │
    └──────┬───────┘       │         └──────┬───────┘
           │               │                │
           │      ┌────────▼──────────┐     │
           ├─────►│ FACT:             │◄────┤
           │      │ sales_transactions│     │
           │      │ (Line Items)      │     │
           │      └────────▲──────────┘     │
           │               │                │
    ┌──────┴────────┐      │       ┌────────┴──────┐
    │ dim_locations │      │       │  dim_staff    │
    │ (Outlets)     │      │       │  (Cashiers)   │
    └───────────────┘      │       └───────────────┘
                           │
                  ┌────────┴────────┐
                  │   dim_time      │
                  │ (Time of Day)   │
                  └─────────────────┘

         Additional Dimensions:
         - dim_promotions (Vouchers)
         - dim_payment_types (Methods)
         - dim_terminals (POS Devices)
```

---

## 📏 Fact Table

### `fact_sales_transactions`

**Purpose**: Central fact table containing all sales line items with proportional payment allocation  
**Grain**: One row per item per payment method (split-tender invoices generate multiple rows)  
**Estimated Rows**: Millions (grows daily)

**Split-Tender Payment Handling** (v1.7.0+):
- Invoices with multiple payment methods (e.g., cash + card) generate **multiple fact rows per line item**
- Each payment method gets its **proportional share** of the line item amount
- Example: RM 100 item paid with RM 30 cash (30%) + RM 70 card (70%) → 2 fact rows:
  - Row 1: Cash, TotalAmount = RM 30
  - Row 2: Card, TotalAmount = RM 70
- This ensures accurate payment method breakdowns in all reports

#### Schema

| Column | Data Type | Key | Description |
|--------|-----------|-----|-------------|
| `SalesItemKey` | INT | PK | Surrogate key (auto-increment) |
| `DateKey` | INT | FK | Foreign key to `dim_date` (YYYYMMDD format) |
| `TimeKey` | INT | FK | Foreign key to `dim_time` (HHMMSS format) |
| `LocationKey` | INT | FK | Foreign key to `dim_locations` |
| `ProductKey` | INT | FK | Foreign key to `dim_products` |
| `CustomerKey` | INT | FK | Foreign key to `dim_customers` (-1 for unknown) |
| `StaffKey` | INT | FK | Foreign key to `dim_staff` (-1 for unspecified) |
| `PromotionKey` | INT | FK | Foreign key to `dim_promotions` (-1 for none) |
| `PaymentTypeKey` | INT | FK | Foreign key to `dim_payment_types` (-1 for unknown) |
| `TerminalKey` | INT | FK | Foreign key to `dim_terminals` (-1 for unknown) |
| `SaleNumber` | VARCHAR(45) | - | Receipt number (degenerate dimension) |
| `SaleType` | VARCHAR(50) | - | 'Sale', 'Return', 'Dine In', 'Take Away', etc. |
| `OrderSource` | VARCHAR(255) | - | 'XILNEXKIOSK', 'XILNEXLIVESALES', etc. |
| `Quantity` | INT | - | **MEASURE**: Number of units sold |
| `GrossAmount` | DECIMAL(18,4) | - | **MEASURE**: Price × Quantity |
| `DiscountAmount` | DECIMAL(18,4) | - | **MEASURE**: Total discounts applied |
| `NetAmount` | DECIMAL(18,4) | - | **MEASURE**: Gross - Discount |
| `TaxAmount` | DECIMAL(18,4) | - | **MEASURE**: Tax amount |
| `TotalAmount` | DECIMAL(18,4) | - | **MEASURE**: Net + Tax (proportionally allocated for split-tender) |
| `CostAmount` | DECIMAL(18,4) | - | **MEASURE**: Cost of goods sold (proportionally allocated) |
| `PaymentTypeKey` | INT | FK | Links to `dim_payment_types`; split-tender invoices have multiple fact rows with proportionally allocated amounts (v1.7.0+) |
| `CardType` | VARCHAR(100) | - | Detailed payment method descriptor (e.g., 'VISA', 'MASTERCARD', 'TOUCHNGO') |

#### Key Relationships

```sql
-- Join to get transaction date details
JOIN dim_date ON fact.DateKey = dim_date.DateKey

-- Join to get time of day details
JOIN dim_time ON fact.TimeKey = dim_time.TimeKey

-- Join to get location details
JOIN dim_locations ON fact.LocationKey = dim_locations.LocationKey

-- Join to get product details
JOIN dim_products ON fact.ProductKey = dim_products.ProductKey

-- Join to get customer details (handle -1 for unknown)
LEFT JOIN dim_customers ON fact.CustomerKey = dim_customers.CustomerKey

-- Join to get staff details (handle -1 for unspecified)
LEFT JOIN dim_staff ON fact.StaffKey = dim_staff.StaffKey

-- Join to get promotion details (handle -1 for no promotion)
LEFT JOIN dim_promotions ON fact.PromotionKey = dim_promotions.PromotionKey

-- Join to get payment type details (handle -1 for unknown)
LEFT JOIN dim_payment_types ON fact.PaymentTypeKey = dim_payment_types.PaymentTypeKey

-- Join to get terminal details (handle -1 for unknown)
LEFT JOIN dim_terminals ON fact.TerminalKey = dim_terminals.TerminalKey
```

#### Business Rules

**Returns Handling**:
```sql
-- Filter out returns from sales calculations
WHERE SaleType = 'Sale'

-- Or analyze returns separately
WHERE SaleType = 'Return'
```

**Anonymous Sales**:
```sql
-- Sales without loyalty card
WHERE CustomerKey = -1
```

**Profit Calculation**:
```sql
-- Gross Profit = NetAmount - CostAmount
SELECT 
    SUM(NetAmount - CostAmount) as gross_profit,
    SUM(NetAmount - CostAmount) / NULLIF(SUM(NetAmount), 0) as margin_percentage
```

---

## 📅 Dimension Tables

### `dim_date`

**Purpose**: Calendar dimension for date-based analysis  
**Grain**: One row per day  
**Rows**: 4,018 (2018-01-01 to 2028-12-31)

#### Schema

| Column | Data Type | Key | Description |
|--------|-----------|-----|-------------|
| `DateKey` | INT | PK | Date in YYYYMMDD format (e.g., 20251007) |
| `FullDate` | DATE | - | Actual date value |
| `DayOfWeek` | VARCHAR(10) | - | 'Monday', 'Tuesday', etc. |
| `DayOfMonth` | INT | - | 1-31 |
| `MonthNumber` | INT | - | 1-12 |
| `MonthName` | VARCHAR(10) | - | 'January', 'February', etc. |
| `Quarter` | INT | - | 1-4 |
| `Year` | INT | - | 2018-2028 |
| `IsWeekend` | BIT | - | 1 if Saturday/Sunday, 0 otherwise |

#### Common Queries

```sql
-- Sales by day of week
SELECT 
    d.DayOfWeek,
    SUM(f.NetAmount) as total_sales
FROM fact_sales_transactions f
JOIN dim_date d ON f.DateKey = d.DateKey
GROUP BY d.DayOfWeek;

-- Weekend vs Weekday comparison
SELECT 
    CASE WHEN d.IsWeekend = 1 THEN 'Weekend' ELSE 'Weekday' END as period,
    SUM(f.NetAmount) as total_sales
FROM fact_sales_transactions f
JOIN dim_date d ON f.DateKey = d.DateKey
GROUP BY d.IsWeekend;

-- Monthly trend
SELECT 
    d.Year,
    d.MonthName,
    SUM(f.NetAmount) as total_sales
FROM fact_sales_transactions f
JOIN dim_date d ON f.DateKey = d.DateKey
GROUP BY d.Year, d.MonthNumber, d.MonthName
ORDER BY d.Year, d.MonthNumber;
```

---

### `dim_time`

**Purpose**: Time-of-day dimension for hourly analysis  
**Grain**: One row per second  
**Rows**: 86,400 (every second in 24 hours)

#### Schema

| Column | Data Type | Key | Description |
|--------|-----------|-----|-------------|
| `TimeKey` | INT | PK | Time in HHMMSS format (e.g., 143000 = 2:30 PM) |
| `FullTime` | TIME | - | Actual time value |
| `Hour` | INT | - | 0-23 |
| `Minute` | INT | - | 0-59 |
| `Second` | INT | - | 0-59 |
| `TimeOfDayBand` | VARCHAR(20) | - | 'Morning', 'Afternoon', 'Evening', 'Night' |

#### Time Band Definitions

| Band | Hour Range |
|------|------------|
| Morning | 05:00 - 11:59 |
| Afternoon | 12:00 - 16:59 |
| Evening | 17:00 - 20:59 |
| Night | 21:00 - 04:59 |

#### Common Queries

```sql
-- Sales by hour
SELECT 
    t.Hour,
    SUM(f.NetAmount) as total_sales
FROM fact_sales_transactions f
JOIN dim_time t ON f.TimeKey = t.TimeKey
GROUP BY t.Hour
ORDER BY t.Hour;

-- Peak hours analysis
SELECT 
    t.TimeOfDayBand,
    COUNT(*) as transaction_count,
    SUM(f.NetAmount) as total_sales
FROM fact_sales_transactions f
JOIN dim_time t ON f.TimeKey = t.TimeKey
GROUP BY t.TimeOfDayBand;
```

---

### `dim_products`

**Purpose**: Product catalog master list  
**Grain**: One row per unique product/SKU  
**Rows**: ~2,000

#### Schema

| Column | Data Type | Key | Description |
|--------|-----------|-----|-------------|
| `ProductKey` | INT | PK | Surrogate key (auto-increment) |
| `SourceProductID` | BIGINT | UK | Original ID from Xilnex `APP_4_ITEM.ID` |
| `ProductCode` | VARCHAR(255) | UK | Alphanumeric SKU |
| `ProductName` | VARCHAR(255) | - | Display name |
| `Category` | VARCHAR(100) | - | Product category (e.g., 'Ayam Goreng Meals') |
| `ProductType` | VARCHAR(100) | - | 'Food', 'BYOD', etc. |
| `Brand` | VARCHAR(100) | - | Brand name |
| `CurrentSalePrice` | DECIMAL(10,2) | - | Current retail price |
| `IsPackage` | BIT | - | 1 if combo meal, 0 if single item |
| `IsActive` | BIT | - | 1 if currently sold, 0 if discontinued |

#### Common Queries

```sql
-- Top-selling products
SELECT 
    p.ProductName,
    SUM(f.Quantity) as units_sold,
    SUM(f.NetAmount) as revenue
FROM fact_sales_transactions f
JOIN dim_products p ON f.ProductKey = p.ProductKey
WHERE f.SaleType = 'Sale'
GROUP BY p.ProductName
ORDER BY revenue DESC;

-- Category performance
SELECT 
    p.Category,
    COUNT(DISTINCT p.ProductKey) as product_count,
    SUM(f.NetAmount) as total_revenue
FROM fact_sales_transactions f
JOIN dim_products p ON f.ProductKey = p.ProductKey
WHERE f.SaleType = 'Sale'
GROUP BY p.Category;

-- Combo vs Single item sales
SELECT 
    CASE WHEN p.IsPackage = 1 THEN 'Combo Meals' ELSE 'Single Items' END as type,
    SUM(f.NetAmount) as revenue
FROM fact_sales_transactions f
JOIN dim_products p ON f.ProductKey = p.ProductKey
GROUP BY p.IsPackage;
```

---

### `dim_customers`

**Purpose**: Loyalty program member master list  
**Grain**: One row per registered customer  
**Rows**: ~100,000

#### Schema

| Column | Data Type | Key | Description |
|--------|-----------|-----|-------------|
| `CustomerKey` | INT | PK | Surrogate key (auto-increment) |
| `CustomerGUID` | BIGINT | UK | Original ID from Xilnex `APP_4_CUSTOMER.ID` |
| `CustomerCode` | VARCHAR(255) | - | Business-facing customer code |
| `FullName` | VARCHAR(510) | - | Complete customer name |
| `FirstName` | VARCHAR(255) | - | Parsed first name |
| `LastName` | VARCHAR(255) | - | Parsed last name |
| `MobileNumber` | VARCHAR(50) | - | Primary contact number |
| `Email` | VARCHAR(255) | - | Email address |
| `CustomerGroup` | VARCHAR(255) | - | Segmentation group (e.g., 'VIP') |
| `CurrentLoyaltyPoints` | DECIMAL(18,4) | - | Current point balance |
| `RegistrationDate` | DATE | - | Date registered in system |
| `DateOfBirth` | DATE | - | Birthday (may be NULL) |
| `IsActive` | BIT | - | 1 if active, 0 if inactive |

#### Special Record

**CustomerKey = -1**: Represents "Unknown Customer" (anonymous sales)

#### Common Queries

```sql
-- Customer purchase frequency
SELECT 
    c.CustomerKey,
    c.FullName,
    COUNT(DISTINCT f.SaleNumber) as transaction_count,
    SUM(f.NetAmount) as lifetime_value
FROM fact_sales_transactions f
JOIN dim_customers c ON f.CustomerKey = c.CustomerKey
WHERE c.CustomerKey <> -1  -- Exclude anonymous
GROUP BY c.CustomerKey, c.FullName
ORDER BY lifetime_value DESC;

-- Anonymous vs Loyalty sales
SELECT 
    CASE WHEN c.CustomerKey = -1 THEN 'Anonymous' ELSE 'Loyalty Member' END as customer_type,
    COUNT(DISTINCT f.SaleNumber) as transactions,
    SUM(f.NetAmount) as revenue
FROM fact_sales_transactions f
LEFT JOIN dim_customers c ON f.CustomerKey = c.CustomerKey
GROUP BY CASE WHEN c.CustomerKey = -1 THEN 'Anonymous' ELSE 'Loyalty Member' END;
```

---

### `dim_locations`

**Purpose**: Store outlet master list  
**Grain**: One row per physical location  
**Rows**: ~200

#### Schema

| Column | Data Type | Key | Description |
|--------|-----------|-----|-------------|
| `LocationKey` | INT | PK | Surrogate key (auto-increment) |
| `LocationGUID` | VARCHAR(255) | UK | Original GUID from Xilnex |
| `LocationName` | VARCHAR(255) | - | Store name (e.g., 'MB IOI KULAI') |
| `FullAddress` | VARCHAR(1000) | - | Complete address string |
| `City` | VARCHAR(100) | - | Parsed city name |
| `State` | VARCHAR(100) | - | Parsed state name |
| `Postcode` | VARCHAR(10) | - | Postal code |
| `IsActive` | BIT | - | 1 if operating, 0 if closed |

#### Special Record

**LocationKey = -1**: Represents "Unknown Location" (required for referential integrity in other dimensions)

#### Common Queries

```sql
-- Sales by location
SELECT 
    l.LocationName,
    l.State,
    SUM(f.NetAmount) as total_sales,
    COUNT(DISTINCT f.SaleNumber) as transactions
FROM fact_sales_transactions f
JOIN dim_locations l ON f.LocationKey = l.LocationKey
WHERE l.IsActive = 1
GROUP BY l.LocationName, l.State
ORDER BY total_sales DESC;

-- State-level aggregation
SELECT 
    l.State,
    COUNT(DISTINCT l.LocationKey) as store_count,
    SUM(f.NetAmount) as total_sales
FROM fact_sales_transactions f
JOIN dim_locations l ON f.LocationKey = l.LocationKey
GROUP BY l.State;
```

---

### `dim_staff`

**Purpose**: Staff/cashier master list  
**Grain**: One row per staff member or system  
**Rows**: ~1,000

#### Schema

| Column | Data Type | Key | Description |
|--------|-----------|-----|-------------|
| `StaffKey` | INT | PK | Surrogate key (auto-increment) |
| `StaffUsername` | VARCHAR(255) | UK | Username (natural key) |
| `StaffFullName` | VARCHAR(255) | - | Display name |
| `StaffType` | VARCHAR(50) | - | 'Human', 'Integration', 'System' |

#### Staff Type Categories

| StaffType | Description | Examples |
|-----------|-------------|----------|
| Human | Regular employees | Cashiers, managers |
| Integration | Third-party platforms | FoodPanda, GrabFood |
| System | Automated systems | Self-service kiosk |

#### Special Record

**StaffKey = -1**: Represents "Unspecified Staff"

#### Common Queries

```sql
-- Staff performance
SELECT 
    s.StaffFullName,
    s.StaffType,
    COUNT(DISTINCT f.SaleNumber) as transactions_handled,
    SUM(f.NetAmount) as sales_value
FROM fact_sales_transactions f
JOIN dim_staff s ON f.StaffKey = s.StaffKey
WHERE s.StaffKey <> -1  -- Exclude unspecified
GROUP BY s.StaffFullName, s.StaffType
ORDER BY sales_value DESC;

-- Sales channel comparison
SELECT 
    s.StaffType,
    SUM(f.NetAmount) as revenue
FROM fact_sales_transactions f
JOIN dim_staff s ON f.StaffKey = s.StaffKey
GROUP BY s.StaffType;
```

---

### `dim_promotions`

**Purpose**: Promotion and voucher master list  
**Grain**: One row per promotion/voucher  
**Rows**: ~500

#### Schema

| Column | Data Type | Key | Description |
|--------|-----------|-----|-------------|
| `PromotionKey` | INT | PK | Surrogate key (auto-increment) |
| `SourcePromotionID` | BIGINT | UK | Original ID from Xilnex |
| `PromotionName` | VARCHAR(255) | - | Display name |
| `PromotionDescription` | VARCHAR(MAX) | - | Detailed description |
| `PromotionCode` | VARCHAR(255) | - | Voucher code |
| `PromotionType` | VARCHAR(50) | - | 'Product Deal', 'Discount Voucher', 'Gift Voucher' |
| `StartDate` | DATE | - | Promotion start date |
| `EndDate` | DATE | - | Promotion end date |
| `IsActive` | BIT | - | 1 if currently active, 0 if expired |

#### Special Record

**PromotionKey = -1**: Represents "No Promotion"

#### Common Queries

```sql
-- Promotion effectiveness
SELECT 
    pr.PromotionName,
    pr.PromotionType,
    COUNT(f.SalesItemKey) as times_used,
    SUM(f.DiscountAmount) as total_discount,
    SUM(f.NetAmount) as revenue_generated
FROM fact_sales_transactions f
JOIN dim_promotions pr ON f.PromotionKey = pr.PromotionKey
WHERE pr.PromotionKey <> -1  -- Exclude "No Promotion"
GROUP BY pr.PromotionName, pr.PromotionType
ORDER BY revenue_generated DESC;
```

---

### `dim_payment_types`

**Purpose**: Payment method master list  
**Grain**: One row per payment method  
**Rows**: ~10

#### Schema

| Column | Data Type | Key | Description |
|--------|-----------|-----|-------------|
| `PaymentTypeKey` | INT | PK | Surrogate key (auto-increment) |
| `PaymentMethodName` | VARCHAR(255) | UK | Method name (e.g., 'cash', 'card') |
| `PaymentCategory` | VARCHAR(50) | - | High-level grouping |

#### Payment Categories

| PaymentCategory | Examples |
|-----------------|----------|
| Cash | 'cash' |
| Card | 'card' |
| E-Wallet | 'ewallet' |
| Voucher | 'voucher', 'forfeiture_voucher' |
| Other | Other methods |

#### Common Queries

```sql
-- Payment method distribution
SELECT 
    pt.PaymentMethodName,
    pt.PaymentCategory,
    COUNT(DISTINCT f.SaleNumber) as transactions,
    SUM(f.TotalAmount) as total_value
FROM fact_sales_transactions f
JOIN dim_payment_types pt ON f.PaymentTypeKey = pt.PaymentTypeKey
GROUP BY pt.PaymentMethodName, pt.PaymentCategory
ORDER BY total_value DESC;
```

---

### `dim_terminals`

**Purpose**: POS terminal/device master list  
**Grain**: One row per terminal device  
**Rows**: Variable (depends on number of terminals across locations)

#### Schema

| Column | Data Type | Key | Description |
|--------|-----------|-----|-------------|
| `TerminalKey` | INT | PK | Surrogate key (auto-increment) |
| `TerminalID` | VARCHAR(50) | UK | Terminal identifier from POS system |
| `LocationKey` | INT | FK | Foreign key to `dim_locations` |
| `TerminalName` | VARCHAR(100) | - | Display name of the terminal |
| `IsActive` | BIT | - | 1 if currently active, 0 if deactivated |

#### Special Record

**TerminalKey = -1**: Represents "Unknown Terminal"

#### Common Queries

```sql
-- Sales by terminal
SELECT 
    term.TerminalID,
    term.TerminalName,
    l.LocationName,
    COUNT(DISTINCT f.SaleNumber) as transactions,
    SUM(f.NetAmount) as total_sales
FROM fact_sales_transactions f
JOIN dim_terminals term ON f.TerminalKey = term.TerminalKey
JOIN dim_locations l ON term.LocationKey = l.LocationKey
WHERE term.TerminalKey <> -1  -- Exclude unknown
GROUP BY term.TerminalID, term.TerminalName, l.LocationName
ORDER BY total_sales DESC;

-- Terminal performance by location
SELECT 
    l.LocationName,
    COUNT(DISTINCT term.TerminalKey) as terminal_count,
    SUM(f.NetAmount) as total_sales
FROM fact_sales_transactions f
JOIN dim_terminals term ON f.TerminalKey = term.TerminalKey
JOIN dim_locations l ON term.LocationKey = l.LocationKey
WHERE term.TerminalKey <> -1
GROUP BY l.LocationName;
```

---

## 🎯 Query Patterns & Best Practices

### 1. Date Range Queries

```sql
-- Using DateKey for efficient filtering
SELECT 
    d.FullDate,
    SUM(f.NetAmount) as daily_sales
FROM fact_sales_transactions f
JOIN dim_date d ON f.DateKey = d.DateKey
WHERE d.DateKey BETWEEN 20250101 AND 20250131  -- January 2025
GROUP BY d.FullDate;
```

### 2. Multi-Dimensional Analysis

```sql
-- Sales by location, product category, and time band
SELECT 
    l.LocationName,
    p.Category,
    t.TimeOfDayBand,
    SUM(f.NetAmount) as sales,
    COUNT(DISTINCT f.SaleNumber) as transactions
FROM fact_sales_transactions f
JOIN dim_locations l ON f.LocationKey = l.LocationKey
JOIN dim_products p ON f.ProductKey = p.ProductKey
JOIN dim_time t ON f.TimeKey = t.TimeKey
JOIN dim_date d ON f.DateKey = d.DateKey
WHERE d.DateKey >= 20250101
  AND f.SaleType = 'Sale'
GROUP BY l.LocationName, p.Category, t.TimeOfDayBand;
```

### 3. Handling Default Values

```sql
-- Always handle -1 surrogate keys appropriately
SELECT 
    COALESCE(c.FullName, 'Walk-in Customer') as customer_name,
    SUM(f.NetAmount) as total_sales
FROM fact_sales_transactions f
LEFT JOIN dim_customers c ON f.CustomerKey = c.CustomerKey
GROUP BY COALESCE(c.FullName, 'Walk-in Customer');
```

---

## 📊 Common Business Metrics

### Total Sales Revenue
```sql
SELECT SUM(NetAmount) as total_revenue
FROM fact_sales_transactions
WHERE SaleType = 'Sale';
```

### Average Transaction Value
```sql
SELECT 
    SUM(NetAmount) / COUNT(DISTINCT SaleNumber) as avg_transaction_value
FROM fact_sales_transactions
WHERE SaleType = 'Sale';
```

### Customer Lifetime Value
```sql
SELECT 
    CustomerKey,
    SUM(NetAmount) as lifetime_value,
    COUNT(DISTINCT SaleNumber) as visit_count
FROM fact_sales_transactions
WHERE CustomerKey <> -1  -- Exclude anonymous
GROUP BY CustomerKey;
```

### Gross Margin
```sql
SELECT 
    SUM(NetAmount - CostAmount) as gross_profit,
    (SUM(NetAmount - CostAmount) / NULLIF(SUM(NetAmount), 0)) * 100 as margin_percent
FROM fact_sales_transactions
WHERE SaleType = 'Sale';
```

---

## 🔑 Index Strategy

### Fact Table Indexes

```sql
-- Clustered index on surrogate key
CREATE CLUSTERED INDEX IX_FactSales_SalesItemKey 
ON fact_sales_transactions(SalesItemKey);

-- Covering index for date-based queries
CREATE NONCLUSTERED INDEX IX_FactSales_DateKey 
ON fact_sales_transactions(DateKey) 
INCLUDE (NetAmount, Quantity, SaleNumber);

-- Covering index for location-based queries
CREATE NONCLUSTERED INDEX IX_FactSales_LocationKey 
ON fact_sales_transactions(LocationKey) 
INCLUDE (DateKey, NetAmount);
```

### Dimension Table Indexes

```sql
-- Primary key indexes (automatically created)
-- Additional indexes on foreign key lookup columns

CREATE INDEX IX_Products_SourceProductID 
ON dim_products(SourceProductID);

CREATE INDEX IX_Customers_CustomerGUID 
ON dim_customers(CustomerGUID);
```

---

## 📚 Source System Mapping

| Target Table | Source Table(s) | ETL Pattern |
|--------------|-----------------|-------------|
| `fact_sales_transactions` | `APP_4_SALES`, `APP_4_SALESITEM`, `APP_4_PAYMENT` | ELT |
| `dim_products` | `APP_4_ITEM` | ETL |
| `dim_customers` | `APP_4_CUSTOMER` | ETL |
| `dim_locations` | `LOCATION_DETAIL` | ETL |
| `dim_staff` | `APP_4_SALES` (extracted) | ETL |
| `dim_promotions` | `APP_4_VOUCHER_MASTER` | ETL |
| `dim_payment_types` | `APP_4_PAYMENT` (distinct) | ETL |
| `dim_terminals` | `APP_4_CASHIER_DRAWER` | ETL |
| `dim_date` | Generated programmatically | N/A |
| `dim_time` | Generated programmatically | N/A |

---

## 📞 Support

For questions about the schema or data definitions, contact the MIS team or refer to the ETL documentation.

