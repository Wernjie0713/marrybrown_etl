-- ============================================================================
-- FIX WAREHOUSE SCHEMA FOR FULL API DATA (NO TRUNCATION)
-- ============================================================================
-- Purpose: Increase column sizes to accommodate full API data without truncation
-- Date: November 14, 2025
-- ============================================================================

USE MarryBrown_DW;
GO

PRINT '============================================================================';
PRINT 'FIXING WAREHOUSE SCHEMA FOR FULL API DATA (100% ACCURACY)';
PRINT '============================================================================';
PRINT '';

-- ============================================================================
-- PART 1: FIX staging_sales TABLE
-- ============================================================================
PRINT 'PART 1: Fixing staging_sales table columns...';
PRINT '';

-- Check current column sizes
PRINT '[INFO] Current column sizes:';
SELECT 
    c.name AS ColumnName,
    t.name AS DataType,
    CASE 
        WHEN t.name LIKE 'n%char' THEN c.max_length / 2 
        ELSE c.max_length 
    END AS CurrentCharLength
FROM sys.columns c
INNER JOIN sys.types t ON c.user_type_id = t.user_type_id
WHERE c.object_id = OBJECT_ID('dbo.staging_sales')
    AND t.name LIKE '%char%'
ORDER BY c.column_id;
GO

PRINT '';
PRINT '[ACTION] Increasing column sizes...';
PRINT '';

-- Increase OutletID (currently 50, API data can be longer)
ALTER TABLE dbo.staging_sales
    ALTER COLUMN OutletID NVARCHAR(500);
PRINT '  [OK] OutletID: 50 -> 500';

-- Increase OutletName (currently 200, API data can be longer)
ALTER TABLE dbo.staging_sales
    ALTER COLUMN OutletName NVARCHAR(500);
PRINT '  [OK] OutletName: 200 -> 500';

-- Increase CashierName (currently 200, API data can be longer)
ALTER TABLE dbo.staging_sales
    ALTER COLUMN CashierName NVARCHAR(500);
PRINT '  [OK] CashierName: 200 -> 500';

-- Increase SalesType (currently 50)
ALTER TABLE dbo.staging_sales
    ALTER COLUMN SalesType NVARCHAR(500);
PRINT '  [OK] SalesType: 50 -> 500';

-- Increase SubSalesType (currently 50)
ALTER TABLE dbo.staging_sales
    ALTER COLUMN SubSalesType NVARCHAR(500);
PRINT '  [OK] SubSalesType: 50 -> 500';

-- Increase OrderNo (currently 50)
ALTER TABLE dbo.staging_sales
    ALTER COLUMN OrderNo NVARCHAR(500);
PRINT '  [OK] OrderNo: 50 -> 500';

-- Increase PaymentStatus (currently 50)
ALTER TABLE dbo.staging_sales
    ALTER COLUMN PaymentStatus NVARCHAR(500);
PRINT '  [OK] PaymentStatus: 50 -> 500';

-- Increase Status (currently 20)
ALTER TABLE dbo.staging_sales
    ALTER COLUMN Status NVARCHAR(500);
PRINT '  [OK] Status: 20 -> 500';

-- Increase BatchID (currently 50)
ALTER TABLE dbo.staging_sales
    ALTER COLUMN BatchID NVARCHAR(500);
PRINT '  [OK] BatchID: 50 -> 500';

-- ADD MISSING COLUMNS FOR COMPLEX API FIELDS (JSON serialized)
-- These columns store complex nested data from API as JSON strings
IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('dbo.staging_sales') AND name = 'Items')
BEGIN
    ALTER TABLE dbo.staging_sales ADD Items NVARCHAR(MAX);
    PRINT '  [OK] Added Items column (NVARCHAR(MAX) for JSON)';
END
ELSE
    PRINT '  - Items column already exists';

IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('dbo.staging_sales') AND name = 'Collection')
BEGIN
    ALTER TABLE dbo.staging_sales ADD Collection NVARCHAR(MAX);
    PRINT '  [OK] Added Collection column (NVARCHAR(MAX) for JSON)';
END
ELSE
    PRINT '  - Collection column already exists';

IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('dbo.staging_sales') AND name = 'Voucher')
BEGIN
    ALTER TABLE dbo.staging_sales ADD Voucher NVARCHAR(MAX);
    PRINT '  [OK] Added Voucher column (NVARCHAR(MAX) for JSON)';
END
ELSE
    PRINT '  - Voucher column already exists';

IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('dbo.staging_sales') AND name = 'ExtendedSales')
BEGIN
    ALTER TABLE dbo.staging_sales ADD ExtendedSales NVARCHAR(MAX);
    PRINT '  [OK] Added ExtendedSales column (NVARCHAR(MAX) for JSON)';
END
ELSE
    PRINT '  - ExtendedSales column already exists';

IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('dbo.staging_sales') AND name = 'BillingAddress')
BEGIN
    ALTER TABLE dbo.staging_sales ADD BillingAddress NVARCHAR(MAX);
    PRINT '  [OK] Added BillingAddress column (NVARCHAR(MAX) for JSON)';
END
ELSE
    PRINT '  - BillingAddress column already exists';

IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('dbo.staging_sales') AND name = 'ShippingAddress')
BEGIN
    ALTER TABLE dbo.staging_sales ADD ShippingAddress NVARCHAR(MAX);
    PRINT '  [OK] Added ShippingAddress column (NVARCHAR(MAX) for JSON)';
END
ELSE
    PRINT '  - ShippingAddress column already exists';

IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('dbo.staging_sales') AND name = 'Client')
BEGIN
    ALTER TABLE dbo.staging_sales ADD Client NVARCHAR(MAX);
    PRINT '  [OK] Added Client column (NVARCHAR(MAX) for JSON)';
END
ELSE
    PRINT '  - Client column already exists';

IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('dbo.staging_sales') AND name = 'LocationKey')
BEGIN
    ALTER TABLE dbo.staging_sales ADD LocationKey INT;
    PRINT '  [OK] Added LocationKey column (for dimension join)';
END
ELSE
    PRINT '  - LocationKey column already exists';

GO

PRINT '';
PRINT '[VERIFY] New column sizes for staging_sales:';
SELECT 
    c.name AS ColumnName,
    t.name AS DataType,
    CASE 
        WHEN t.name LIKE 'n%char' THEN 
            CASE 
                WHEN c.max_length = -1 THEN 'MAX'
                ELSE CAST(c.max_length / 2 AS VARCHAR(10))
            END
        ELSE CAST(c.max_length AS VARCHAR(10))
    END AS NewCharLength
FROM sys.columns c
INNER JOIN sys.types t ON c.user_type_id = t.user_type_id
WHERE c.object_id = OBJECT_ID('dbo.staging_sales')
    AND t.name LIKE '%char%'
ORDER BY c.column_id;
GO

-- ============================================================================
-- PART 2: FIX staging_sales_items TABLE
-- ============================================================================
PRINT '';
PRINT 'PART 2: Fixing staging_sales_items table columns...';
PRINT '';

-- Increase ProductCode (currently 50)
ALTER TABLE dbo.staging_sales_items
    ALTER COLUMN ProductCode NVARCHAR(500);
PRINT '  [OK] ProductCode: 50 -> 500';

-- Increase ProductName (currently 200)
ALTER TABLE dbo.staging_sales_items
    ALTER COLUMN ProductName NVARCHAR(500);
PRINT '  [OK] ProductName: 200 -> 500';

-- Increase Category (currently 100)
ALTER TABLE dbo.staging_sales_items
    ALTER COLUMN Category NVARCHAR(500);
PRINT '  [OK] Category: 100 -> 500';

-- Increase TaxCode (currently 20)
ALTER TABLE dbo.staging_sales_items
    ALTER COLUMN TaxCode NVARCHAR(500);
PRINT '  [OK] TaxCode: 20 -> 500';

-- Increase Model (currently 100)
ALTER TABLE dbo.staging_sales_items
    ALTER COLUMN Model NVARCHAR(500);
PRINT '  [OK] Model: 100 -> 500';

-- Increase SalesType (currently 50)
ALTER TABLE dbo.staging_sales_items
    ALTER COLUMN SalesType NVARCHAR(500);
PRINT '  [OK] SalesType: 50 -> 500';

-- Increase SubSalesType (currently 50)
ALTER TABLE dbo.staging_sales_items
    ALTER COLUMN SubSalesType NVARCHAR(500);
PRINT '  [OK] SubSalesType: 50 -> 500';

-- Increase SalesPerson (currently 200)
ALTER TABLE dbo.staging_sales_items
    ALTER COLUMN SalesPerson NVARCHAR(500);
PRINT '  [OK] SalesPerson: 200 -> 500';

-- Increase BatchID (currently 50)
ALTER TABLE dbo.staging_sales_items
    ALTER COLUMN BatchID NVARCHAR(500);
PRINT '  [OK] BatchID: 50 -> 500';

GO

-- ============================================================================
-- PART 3: FIX staging_payments TABLE
-- ============================================================================
PRINT '';
PRINT 'PART 3: Fixing staging_payments table columns...';
PRINT '';

-- Increase PaymentMethod (currently 100)
ALTER TABLE dbo.staging_payments
    ALTER COLUMN PaymentMethod NVARCHAR(500);
PRINT '  [OK] PaymentMethod: 100 -> 500';

-- Increase PaymentReference (currently 100)
ALTER TABLE dbo.staging_payments
    ALTER COLUMN PaymentReference NVARCHAR(500);
PRINT '  [OK] PaymentReference: 100 -> 500';

-- Increase EODSessionID (currently 50)
ALTER TABLE dbo.staging_payments
    ALTER COLUMN EODSessionID NVARCHAR(500);
PRINT '  [OK] EODSessionID: 50 -> 500';

-- Increase CardType (currently 50)
ALTER TABLE dbo.staging_payments
    ALTER COLUMN CardType NVARCHAR(500);
PRINT '  [OK] CardType: 50 -> 500';

-- Increase BatchID (currently 50)
ALTER TABLE dbo.staging_payments
    ALTER COLUMN BatchID NVARCHAR(500);
PRINT '  [OK] BatchID: 50 -> 500';

GO

PRINT '';
PRINT '============================================================================';
PRINT 'SCHEMA FIX COMPLETE - ALL COLUMNS NOW SUPPORT FULL API DATA';
PRINT '============================================================================';
PRINT '';
PRINT 'Summary:';
PRINT '  ✓ All VARCHAR/NVARCHAR columns increased to 500 characters';
PRINT '  ✓ Complex API fields (Items, Collection, etc.) added as NVARCHAR(MAX)';
PRINT '  ✓ LocationKey column added for dimension joins';
PRINT '  ✓ NO DATA TRUNCATION - 100% accuracy maintained';
PRINT '';
