-- ============================================================================
-- CLEAR SQL SERVER CACHE AND REBUILD STAGING TABLES
-- ============================================================================
-- This script will:
-- 1. Clear SQL Server metadata cache
-- 2. Drop and recreate staging tables with correct schema
-- ============================================================================

USE MarryBrown_DW;
GO

PRINT '============================================================================';
PRINT 'CLEARING SQL SERVER CACHE AND REBUILDING STAGING TABLES';
PRINT '============================================================================';
PRINT '';

-- ============================================================================
-- STEP 1: Clear SQL Server metadata cache
-- ============================================================================
PRINT 'STEP 1: Clearing SQL Server metadata cache...';
PRINT '';

DBCC FREEPROCCACHE;
DBCC DROPCLEANBUFFERS;
PRINT '[OK] SQL Server cache cleared';

GO

-- ============================================================================
-- STEP 2: Drop existing staging tables
-- ============================================================================
PRINT '';
PRINT 'STEP 2: Dropping existing staging tables...';
PRINT '';

IF EXISTS (SELECT * FROM sys.tables WHERE name = 'staging_payments')
BEGIN
    DROP TABLE dbo.staging_payments;
    PRINT '[OK] Dropped staging_payments';
END

IF EXISTS (SELECT * FROM sys.tables WHERE name = 'staging_sales_items')
BEGIN
    DROP TABLE dbo.staging_sales_items;
    PRINT '[OK] Dropped staging_sales_items';
END

IF EXISTS (SELECT * FROM sys.tables WHERE name = 'staging_sales')
BEGIN
    DROP TABLE dbo.staging_sales;
    PRINT '[OK] Dropped staging_sales';
END

GO

-- ============================================================================
-- STEP 3: Recreate staging tables with CORRECT schema
-- ============================================================================
PRINT '';
PRINT 'STEP 3: Recreating staging tables with correct schema...';
PRINT '';

-- staging_sales (with MAX columns for complex fields)
CREATE TABLE dbo.staging_sales (
    SaleID NVARCHAR(500),
    BusinessDateTime DATETIME,
    SystemDateTime DATETIME,
    OutletID NVARCHAR(500),
    OutletName NVARCHAR(500),
    CashierName NVARCHAR(500),
    SalesType NVARCHAR(500),
    SubSalesType NVARCHAR(500),
    GrandTotal DECIMAL(18,2),
    NetAmount DECIMAL(18,2),
    TaxAmount DECIMAL(18,2),
    Paid DECIMAL(18,2),
    Balance DECIMAL(18,2),
    Rounding DECIMAL(18,2),
    PaxNumber INT,
    BillDiscountAmount DECIMAL(18,2),
    OrderNo NVARCHAR(500),
    PaymentStatus NVARCHAR(500),
    Status NVARCHAR(500),
    BatchID NVARCHAR(500),
    -- Complex fields as NVARCHAR(MAX)
    Items NVARCHAR(MAX),
    Collection NVARCHAR(MAX),
    Voucher NVARCHAR(MAX),
    ExtendedSales NVARCHAR(MAX),
    BillingAddress NVARCHAR(MAX),
    ShippingAddress NVARCHAR(MAX),
    Client NVARCHAR(MAX),
    LocationKey INT,
    LoadedAt DATETIME DEFAULT GETDATE()
);
PRINT '[OK] Created staging_sales';

GO

-- staging_sales_items
CREATE TABLE dbo.staging_sales_items (
    ItemID NVARCHAR(500),
    SaleID NVARCHAR(500),
    ProductID NVARCHAR(500),
    ProductCode NVARCHAR(500),
    ProductName NVARCHAR(500),
    Category NVARCHAR(500),
    Quantity DECIMAL(18,3),
    UnitPrice DECIMAL(18,2),
    Subtotal DECIMAL(18,2),
    DiscountAmount DECIMAL(18,2),
    NetAmount DECIMAL(18,2),
    TaxAmount DECIMAL(18,2),
    TotalAmount DECIMAL(18,2),
    TaxCode NVARCHAR(500),
    TaxRate DECIMAL(18,4),
    Cost DECIMAL(18,2),
    IsFOC BIT,
    Model NVARCHAR(500),
    IsServiceCharge BIT,
    SalesType NVARCHAR(500),
    SubSalesType NVARCHAR(500),
    SalesPerson NVARCHAR(500),
    BatchID NVARCHAR(500),
    LoadedAt DATETIME DEFAULT GETDATE()
);
PRINT '[OK] Created staging_sales_items';

GO

-- staging_payments
CREATE TABLE dbo.staging_payments (
    PaymentID NVARCHAR(500),
    SaleID NVARCHAR(500),
    PaymentMethod NVARCHAR(500),
    Amount DECIMAL(18,2),
    PaymentDateTime DATETIME,
    BusinessDate DATE,
    PaymentReference NVARCHAR(500),
    EODSessionID NVARCHAR(500),
    TenderAmount DECIMAL(18,2),
    ChangeAmount DECIMAL(18,2),
    CardType NVARCHAR(500),
    IsVoid BIT,
    BatchID NVARCHAR(500),
    LoadedAt DATETIME DEFAULT GETDATE()
);
PRINT '[OK] Created staging_payments';

GO

-- ============================================================================
-- STEP 4: Verify new schema
-- ============================================================================
PRINT '';
PRINT 'STEP 4: Verifying new schema...';
PRINT '';

PRINT 'staging_sales columns:';
SELECT 
    c.name AS ColumnName,
    t.name AS DataType,
    CASE 
        WHEN t.name LIKE 'n%char' THEN 
            CASE 
                WHEN c.max_length = -1 THEN 'MAX'
                ELSE CAST(c.max_length / 2 AS VARCHAR(10))
            END
        ELSE 'N/A'
    END AS CharLength
FROM sys.columns c
INNER JOIN sys.types t ON c.user_type_id = t.user_type_id
WHERE c.object_id = OBJECT_ID('dbo.staging_sales')
ORDER BY c.column_id;

GO

PRINT '';
PRINT '============================================================================';
PRINT 'REBUILD COMPLETE - FRESH STAGING TABLES READY';
PRINT '============================================================================';
PRINT '';
PRINT 'Summary:';
PRINT '  ✓ SQL Server cache cleared';
PRINT '  ✓ Old staging tables dropped';
PRINT '  ✓ New staging tables created with correct schema';
PRINT '  ✓ All NVARCHAR columns: 500 characters';
PRINT '  ✓ Complex fields: NVARCHAR(MAX)';
PRINT '';
