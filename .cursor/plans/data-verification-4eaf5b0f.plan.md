<!-- 4eaf5b0f-7196-47b8-a31c-d55de0d987e4 39831cd5-e02a-4ab5-acf6-de4855601375 -->
# Data Verification Plan: Daily Sales Report Comparison

## Objective

Compare sales data for **10/10/2025** at **MB A FAMOSA** store between:

1. Cloud data warehouse (what the API returns)
2. Xilnex source database (raw data)

## SQL Queries to Execute

### Query 1: Cloud Warehouse - API Logic (Run on TARGET database)

This query replicates exactly what the API endpoint returns:

```sql
-- Query 1: Cloud Warehouse Data (FakeRestaurantDB)
-- This matches the API logic in /sales/reports/daily-sales

SELECT
    d.FullDate as date,
    l.LocationName as store_name,
    l.LocationKey,
    COUNT(DISTINCT f.SaleNumber) as transaction_count,
    COUNT(f.SalesItemKey) as line_item_count,
    SUM(f.NetAmount) as sales_amount,
    SUM(f.CostAmount) as cost_amount,
    SUM(f.NetAmount - f.CostAmount) as profit_amount,
    -- Breakdown by amounts
    SUM(f.GrossAmount) as total_gross,
    SUM(f.DiscountAmount) as total_discount,
    SUM(f.TaxAmount) as total_tax,
    SUM(f.TotalAmount) as total_amount
FROM
    dbo.fact_sales_transactions f
JOIN
    dbo.dim_date d ON f.DateKey = d.DateKey
JOIN
    dbo.dim_locations l ON f.LocationKey = l.LocationKey
WHERE
    d.FullDate = '2025-10-10'
    AND l.LocationName LIKE '%A FAMOSA%'  -- Flexible matching
    AND f.SaleType != 'Return'  -- API excludes returns
GROUP BY
    d.FullDate, l.LocationName, l.LocationKey
ORDER BY
    l.LocationName;
```

### Query 2: Xilnex Source Database - Raw Calculation (Run on XILNEX database)

This query calculates the same metrics from source tables using ETL transformation logic:

```sql
-- Query 2: Xilnex Source Data (COM_5013 schema)
-- This replicates the ETL transformation logic

SELECT
    s.DATETIME__SALES_DATE as date,
    loc.LOCATION_NAME as store_name,
    loc.ID as location_guid,
    COUNT(DISTINCT s.sales_no) as transaction_count,
    COUNT(si.id) as line_item_count,
    -- NetAmount calculation (from transform_sales_facts.sql line 58)
    SUM((si.double_price * si.INT_QUANTITY) - si.double_total_discount_amount) as sales_amount,
    -- CostAmount calculation (from transform_sales_facts.sql line 62)
    SUM(si.double_cost * si.INT_QUANTITY) as cost_amount,
    -- Profit = NetAmount - CostAmount
    SUM((si.double_price * si.INT_QUANTITY) - si.double_total_discount_amount) - 
    SUM(si.double_cost * si.INT_QUANTITY) as profit_amount,
    -- Breakdown by amounts
    SUM(si.double_price * si.INT_QUANTITY) as total_gross,
    SUM(si.double_total_discount_amount) as total_discount,
    SUM(si.double_mgst_tax_amount) as total_tax,
    SUM(si.double_sub_total) as total_amount
FROM
    COM_5013.APP_4_SALES s
JOIN
    COM_5013.APP_4_SALESITEM si ON s.sales_no = si.sales_no
JOIN
    COM_5013.LOCATION_DETAIL loc ON s.sale_location = loc.ID
WHERE
    s.DATETIME__SALES_DATE = '2025-10-10'
    AND loc.LOCATION_NAME LIKE '%A FAMOSA%'  -- Flexible matching
    AND s.SALES_TYPE != 'Return'  -- Match API logic
GROUP BY
    s.DATETIME__SALES_DATE, loc.LOCATION_NAME, loc.ID
ORDER BY
    loc.LOCATION_NAME;
```

### Query 3: Detailed Transaction Comparison (Run on both databases)

**Cloud Warehouse Version:**

```sql
-- Query 3A: Cloud Warehouse - Transaction Detail
SELECT
    f.SaleNumber,
    f.SaleType,
    d.FullDate,
    l.LocationName,
    COUNT(*) as line_items,
    SUM(f.NetAmount) as net_amount,
    SUM(f.CostAmount) as cost_amount
FROM
    dbo.fact_sales_transactions f
JOIN
    dbo.dim_date d ON f.DateKey = d.DateKey
JOIN
    dbo.dim_locations l ON f.LocationKey = l.LocationKey
WHERE
    d.FullDate = '2025-10-10'
    AND l.LocationName LIKE '%A FAMOSA%'
GROUP BY
    f.SaleNumber, f.SaleType, d.FullDate, l.LocationName
ORDER BY
    f.SaleNumber;
```

**Xilnex Source Version:**

```sql
-- Query 3B: Xilnex Source - Transaction Detail
SELECT
    s.sales_no,
    s.SALES_TYPE,
    s.DATETIME__SALES_DATE,
    loc.LOCATION_NAME,
    COUNT(*) as line_items,
    SUM((si.double_price * si.INT_QUANTITY) - si.double_total_discount_amount) as net_amount,
    SUM(si.double_cost * si.INT_QUANTITY) as cost_amount
FROM
    COM_5013.APP_4_SALES s
JOIN
    COM_5013.APP_4_SALESITEM si ON s.sales_no = si.sales_no
JOIN
    COM_5013.LOCATION_DETAIL loc ON s.sale_location = loc.ID
WHERE
    s.DATETIME__SALES_DATE = '2025-10-10'
    AND loc.LOCATION_NAME LIKE '%A FAMOSA%'
GROUP BY
    s.sales_no, s.SALES_TYPE, s.DATETIME__SALES_DATE, loc.LOCATION_NAME
ORDER BY
    s.sales_no;
```

### Query 4: Location Name Verification

Check exact location name in both systems:

**Cloud Warehouse:**

```sql
SELECT LocationKey, LocationName, LocationGUID, IsActive
FROM dbo.dim_locations
WHERE LocationName LIKE '%FAMOSA%';
```

**Xilnex:**

```sql
SELECT ID, LOCATION_NAME, LOCATION_DELETED
FROM COM_5013.LOCATION_DETAIL
WHERE LOCATION_NAME LIKE '%FAMOSA%';
```

## Diagnostic Checklist

Compare these key metrics between both queries:

1. **Transaction Count** - Should match exactly
2. **Line Item Count** - Should match exactly
3. **Sales Amount (NetAmount)** - Should match exactly
4. **Profit Amount** - Should match exactly
5. **Location Name** - Verify exact spelling matches

## Common Discrepancy Causes

1. **SaleType filtering** - Xilnex might have different values ('Return' vs 'Refund')
2. **Location name mismatch** - Exact spelling differences
3. **Date format** - Timezone or format differences
4. **ETL timing** - Data not yet processed for that date
5. **Null handling** - Nulls in amounts causing calculation differences

## Execution Instructions

1. **Run Query 1** on cloud warehouse (FakeRestaurantDB)
2. **Run Query 2** on Xilnex database (COM_5013 schema)
3. Compare `sales_amount` and `profit_amount` values
4. If different, run Query 3 to see transaction-level detail
5. Run Query 4 to verify location name exact match
6. Share results for further diagnosis

## Expected Outcome

If ETL is correct: Query 1 = Query 2 (all metrics should match)

If different, the discrepancy indicates:

- **ETL Issue**: Data not loaded or transformation logic incorrect
- **API Issue**: Query logic different from ETL logic (unlikely based on code review)
- **Data Issue**: Source data changed after ETL ran