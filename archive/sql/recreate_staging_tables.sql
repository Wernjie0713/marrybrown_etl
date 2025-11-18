-- Drop and recreate staging tables with correct schema
-- This will force pyodbc to use the new schema

USE MarryBrown_DW;
GO

PRINT '=== Dropping existing staging tables ===';
GO

DROP TABLE IF EXISTS dbo.staging_payments;
PRINT '  [OK] Dropped staging_payments';

DROP TABLE IF EXISTS dbo.staging_sales_items;
PRINT '  [OK] Dropped staging_sales_items';

DROP TABLE IF EXISTS dbo.staging_sales;
PRINT '  [OK] Dropped staging_sales';
GO

PRINT '';
PRINT '=== Creating staging tables with correct schema ===';
GO

-- staging_sales (ALL columns NVARCHAR(100) or larger)
CREATE TABLE dbo.staging_sales (
    SaleID NVARCHAR(100),          -- MongoDB ObjectId (24 chars)
    BusinessDateTime DATETIME,
    SystemDateTime DATETIME,
    OutletID NVARCHAR(100),
    OutletName NVARCHAR(300),
    CashierName NVARCHAR(300),
    SalesType NVARCHAR(100),
    SubSalesType NVARCHAR(100),
    GrandTotal DECIMAL(18,2),
    NetAmount DECIMAL(18,2),
    TaxAmount DECIMAL(18,2),
    Paid DECIMAL(18,2),
    Balance DECIMAL(18,2),
    Rounding DECIMAL(18,2),
    PaxNumber INT,
    BillDiscountAmount DECIMAL(18,2),
    OrderNo NVARCHAR(100),
    PaymentStatus NVARCHAR(100),
    Status NVARCHAR(100),           -- Increased from 50
    BatchID NVARCHAR(100),
    LoadedAt DATETIME DEFAULT GETDATE()
);
PRINT '  [OK] Created staging_sales';
GO

-- staging_sales_items (ALL columns NVARCHAR(100) or larger)
CREATE TABLE dbo.staging_sales_items (
    ItemID NVARCHAR(100),           -- MongoDB ObjectId
    SaleID NVARCHAR(100),           -- MongoDB ObjectId
    ProductID NVARCHAR(100),        -- MongoDB ObjectId
    ProductCode NVARCHAR(100),
    ProductName NVARCHAR(300),
    Category NVARCHAR(100),
    Quantity DECIMAL(18,3),
    UnitPrice DECIMAL(18,2),
    Subtotal DECIMAL(18,2),
    DiscountAmount DECIMAL(18,2),
    NetAmount DECIMAL(18,2),
    TaxAmount DECIMAL(18,2),
    TotalAmount DECIMAL(18,2),
    TaxCode NVARCHAR(100),          -- Increased from 20!
    TaxRate DECIMAL(18,4),
    Cost DECIMAL(18,2),
    IsFOC BIT,
    Model NVARCHAR(100),
    IsServiceCharge BIT,
    SalesType NVARCHAR(100),
    SubSalesType NVARCHAR(100),
    SalesPerson NVARCHAR(300),
    BatchID NVARCHAR(100),
    LoadedAt DATETIME DEFAULT GETDATE()
);
PRINT '  [OK] Created staging_sales_items';
GO

-- staging_payments (ALL columns NVARCHAR(100) or larger)
CREATE TABLE dbo.staging_payments (
    PaymentID NVARCHAR(100),        -- MongoDB ObjectId
    SaleID NVARCHAR(100),           -- MongoDB ObjectId
    PaymentMethod NVARCHAR(100),
    Amount DECIMAL(18,2),
    PaymentDateTime DATETIME,
    BusinessDate DATE,
    PaymentReference NVARCHAR(200),
    EODSessionID NVARCHAR(100),
    TenderAmount DECIMAL(18,2),
    ChangeAmount DECIMAL(18,2),
    CardType NVARCHAR(100),
    IsVoid BIT,
    BatchID NVARCHAR(100),
    LoadedAt DATETIME DEFAULT GETDATE()
);
PRINT '  [OK] Created staging_payments';
GO

PRINT '';
PRINT '=== Verifying new schema ===';
GO

SELECT 
    TABLE_NAME,
    COLUMN_NAME,
    DATA_TYPE,
    CHARACTER_MAXIMUM_LENGTH
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'dbo'
    AND TABLE_NAME LIKE 'staging_%'
    AND DATA_TYPE LIKE '%char%'
ORDER BY TABLE_NAME, ORDINAL_POSITION;
GO

PRINT '';
PRINT '=== Staging tables recreated successfully! ===';
PRINT 'All string columns are now >= 100 characters';
GO

