/*
Create API Testing Fact Table
Date: October 28, 2025
Purpose: Clone fact_sales_transactions with additional fields for API ETL testing

This creates a parallel fact table to test API-based ETL without affecting production data.
*/

USE FakeRestaurantDB;
GO

PRINT 'Creating fact_sales_transactions_api for API ETL testing...';

-- Step 1: Clone structure from existing fact table
IF OBJECT_ID('dbo.fact_sales_transactions_api', 'U') IS NOT NULL
BEGIN
    PRINT '  Dropping existing fact_sales_transactions_api...';
    DROP TABLE dbo.fact_sales_transactions_api;
END

-- Create table with same structure as production fact table
SELECT TOP 0 * 
INTO dbo.fact_sales_transactions_api 
FROM dbo.fact_sales_transactions;

PRINT '  ✓ Base structure cloned from fact_sales_transactions';

-- Step 2: Add new columns from API mapping analysis
ALTER TABLE dbo.fact_sales_transactions_api ADD
    TaxCode VARCHAR(10) NULL,           -- Tax category (SV, ZR, SR)
    TaxRate DECIMAL(5,2) NULL,          -- Tax rate percentage (6.00, 0.00)
    IsFOC BIT NOT NULL DEFAULT 0,       -- Free of charge flag
    Rounding DECIMAL(10,4) NULL,        -- Rounding adjustment
    Model VARCHAR(100) NULL,            -- Product variant (REGULAR, LARGE)
    IsServiceCharge BIT NOT NULL DEFAULT 0;  -- Service charge flag

PRINT '  ✓ Added 6 new columns from API mapping';

-- Step 3: Create indexes for query performance
CREATE CLUSTERED INDEX IX_FactSalesAPI_SalesItemKey 
ON dbo.fact_sales_transactions_api(SalesItemKey);

CREATE NONCLUSTERED INDEX IX_FactSalesAPI_DateKey 
ON dbo.fact_sales_transactions_api(DateKey) 
INCLUDE (LocationKey, TotalAmount, NetAmount, TaxAmount);

CREATE NONCLUSTERED INDEX IX_FactSalesAPI_LocationKey 
ON dbo.fact_sales_transactions_api(LocationKey) 
INCLUDE (DateKey, TotalAmount);

CREATE NONCLUSTERED INDEX IX_FactSalesAPI_SaleNumber 
ON dbo.fact_sales_transactions_api(SaleNumber);

PRINT '  ✓ Created indexes for query performance';

-- Step 4: Add extended properties for documentation
EXEC sys.sp_addextendedproperty 
    @name = N'MS_Description', 
    @value = N'API ETL testing fact table. Clone of fact_sales_transactions with additional API-sourced fields. Used for parallel testing before production cutover.', 
    @level0type = N'SCHEMA', @level0name = 'dbo',
    @level1type = N'TABLE',  @level1name = 'fact_sales_transactions_api';

PRINT '  ✓ Added table description';

-- Step 5: Verify table structure
PRINT '';
PRINT 'Verification:';

-- Check column count
SELECT 
    'fact_sales_transactions_api' as TableName,
    COUNT(*) as ColumnCount
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'fact_sales_transactions_api'
  AND TABLE_SCHEMA = 'dbo';

-- Check row count
SELECT 
    COUNT(*) as InitialRowCount
FROM dbo.fact_sales_transactions_api;

PRINT '';
PRINT '========================================';
PRINT 'fact_sales_transactions_api Created!';
PRINT '========================================';
PRINT 'Ready for API ETL testing with October 2018 data.';
PRINT '';

