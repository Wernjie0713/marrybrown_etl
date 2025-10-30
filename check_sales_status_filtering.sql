-- ============================================================================
-- Investigate Sales Status Filtering - October 2018
-- Check what statuses exist in API data and their impact on totals
-- ============================================================================

USE FakeRestaurantDB;
GO

PRINT '============================================================================';
PRINT 'SALES STATUS ANALYSIS - October 2018';
PRINT '============================================================================';
PRINT '';

-- 1. Overall status breakdown
PRINT '1. Overall Status Breakdown (All Locations)';
PRINT '----------------------------------------------------------------------------';

SELECT 
    ISNULL(f.SalesStatus, 'NULL') as SalesStatus,
    COUNT(DISTINCT f.SaleNumber) as TransactionCount,
    SUM(f.TotalAmount) as TotalSales,
    SUM(f.NetAmount - f.CostAmount) as TotalProfit
FROM dbo.fact_sales_transactions_api f
JOIN dbo.dim_date d ON f.DateKey = d.DateKey
WHERE d.FullDate BETWEEN '2018-10-01' AND '2018-10-31'
GROUP BY f.SalesStatus
ORDER BY TransactionCount DESC;

PRINT '';
PRINT '';

-- 2. Status breakdown by location
PRINT '2. Status Breakdown by Location';
PRINT '----------------------------------------------------------------------------';

SELECT 
    l.LocationName,
    ISNULL(f.SalesStatus, 'NULL') as SalesStatus,
    COUNT(DISTINCT f.SaleNumber) as TransactionCount,
    SUM(f.TotalAmount) as TotalSales,
    SUM(f.NetAmount - f.CostAmount) as TotalProfit
FROM dbo.fact_sales_transactions_api f
JOIN dbo.dim_date d ON f.DateKey = d.DateKey
JOIN dbo.dim_locations l ON f.LocationKey = l.LocationKey
WHERE d.FullDate BETWEEN '2018-10-01' AND '2018-10-31'
GROUP BY l.LocationName, f.SalesStatus
ORDER BY l.LocationName, f.SalesStatus;

PRINT '';
PRINT '';

-- 3. Comparison: ALL statuses vs COMPLETED only
PRINT '3. Impact of Status Filtering (ALL vs COMPLETED)';
PRINT '----------------------------------------------------------------------------';

WITH AllStatuses AS (
    SELECT 
        l.LocationName,
        SUM(f.TotalAmount) as TotalSales_All,
        SUM(f.NetAmount - f.CostAmount) as TotalProfit_All
    FROM dbo.fact_sales_transactions_api f
    JOIN dbo.dim_date d ON f.DateKey = d.DateKey
    JOIN dbo.dim_locations l ON f.LocationKey = l.LocationKey
    WHERE d.FullDate BETWEEN '2018-10-01' AND '2018-10-31'
    GROUP BY l.LocationName
),
CompletedOnly AS (
    SELECT 
        l.LocationName,
        SUM(f.TotalAmount) as TotalSales_Completed,
        SUM(f.NetAmount - f.CostAmount) as TotalProfit_Completed
    FROM dbo.fact_sales_transactions_api f
    JOIN dbo.dim_date d ON f.DateKey = d.DateKey
    JOIN dbo.dim_locations l ON f.LocationKey = l.LocationKey
    WHERE d.FullDate BETWEEN '2018-10-01' AND '2018-10-31'
      AND f.SalesStatus = 'COMPLETED'
    GROUP BY l.LocationName
)
SELECT 
    a.LocationName,
    a.TotalSales_All,
    ISNULL(c.TotalSales_Completed, 0) as TotalSales_Completed,
    a.TotalSales_All - ISNULL(c.TotalSales_Completed, 0) as Sales_Difference,
    a.TotalProfit_All,
    ISNULL(c.TotalProfit_Completed, 0) as TotalProfit_Completed,
    a.TotalProfit_All - ISNULL(c.TotalProfit_Completed, 0) as Profit_Difference
FROM AllStatuses a
LEFT JOIN CompletedOnly c ON a.LocationName = c.LocationName
ORDER BY a.LocationName;

PRINT '';
PRINT '';

-- 4. Check if SalesStatus is populated at all
PRINT '4. Check SalesStatus Population';
PRINT '----------------------------------------------------------------------------';

SELECT 
    CASE 
        WHEN f.SalesStatus IS NULL THEN 'NULL values'
        WHEN f.SalesStatus = '' THEN 'Empty strings'
        ELSE 'Populated'
    END as StatusPopulation,
    COUNT(*) as RowCount,
    COUNT(DISTINCT f.SaleNumber) as UniqueTransactions
FROM dbo.fact_sales_transactions_api f
JOIN dbo.dim_date d ON f.DateKey = d.DateKey
WHERE d.FullDate BETWEEN '2018-10-01' AND '2018-10-31'
GROUP BY 
    CASE 
        WHEN f.SalesStatus IS NULL THEN 'NULL values'
        WHEN f.SalesStatus = '' THEN 'Empty strings'
        ELSE 'Populated'
    END;

PRINT '';
PRINT '';

-- 5. Sample transactions to see status values
PRINT '5. Sample Transactions (First 20)';
PRINT '----------------------------------------------------------------------------';

SELECT TOP 20
    f.SaleNumber,
    d.FullDate as BusinessDate,
    l.LocationName,
    f.SalesStatus,
    f.SaleType,
    f.TotalAmount,
    f.NetAmount - f.CostAmount as Profit
FROM dbo.fact_sales_transactions_api f
JOIN dbo.dim_date d ON f.DateKey = d.DateKey
JOIN dbo.dim_locations l ON f.LocationKey = l.LocationKey
WHERE d.FullDate BETWEEN '2018-10-01' AND '2018-10-31'
ORDER BY f.SaleNumber;

PRINT '';
PRINT '';
PRINT '============================================================================';
PRINT 'ANALYSIS COMPLETE';
PRINT '============================================================================';

