/*
Schema Enhancements for Xilnex Sync API Integration
Date: October 28, 2025
Purpose: Add missing fields identified in API-to-Warehouse mapping analysis

INSTRUCTIONS:
1. Review changes with supervisor
2. Backup database before running
3. Execute scripts in order
4. Update ETL scripts to populate new fields
*/

USE FakeRestaurantDB;
GO

-- ============================================================================
-- PART 1: Enhance fact_sales_transactions
-- ============================================================================

PRINT 'Adding new columns to fact_sales_transactions...';

-- Check if columns exist before adding (idempotent)
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('dbo.fact_sales_transactions') AND name = 'TaxCode')
BEGIN
    ALTER TABLE dbo.fact_sales_transactions
    ADD TaxCode VARCHAR(10) NULL;
    PRINT '  ✓ Added TaxCode';
END
ELSE
    PRINT '  - TaxCode already exists';

IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('dbo.fact_sales_transactions') AND name = 'TaxRate')
BEGIN
    ALTER TABLE dbo.fact_sales_transactions
    ADD TaxRate DECIMAL(5,2) NULL;
    PRINT '  ✓ Added TaxRate';
END
ELSE
    PRINT '  - TaxRate already exists';

IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('dbo.fact_sales_transactions') AND name = 'IsFOC')
BEGIN
    ALTER TABLE dbo.fact_sales_transactions
    ADD IsFOC BIT NOT NULL DEFAULT 0;
    PRINT '  ✓ Added IsFOC (Free of Charge flag)';
END
ELSE
    PRINT '  - IsFOC already exists';

IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('dbo.fact_sales_transactions') AND name = 'Rounding')
BEGIN
    ALTER TABLE dbo.fact_sales_transactions
    ADD Rounding DECIMAL(10,4) NULL;
    PRINT '  ✓ Added Rounding';
END
ELSE
    PRINT '  - Rounding already exists';

IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('dbo.fact_sales_transactions') AND name = 'Model')
BEGIN
    ALTER TABLE dbo.fact_sales_transactions
    ADD Model VARCHAR(100) NULL;
    PRINT '  ✓ Added Model (product variant: REGULAR, LARGE, etc.)';
END
ELSE
    PRINT '  - Model already exists';

IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('dbo.fact_sales_transactions') AND name = 'IsServiceCharge')
BEGIN
    ALTER TABLE dbo.fact_sales_transactions
    ADD IsServiceCharge BIT NOT NULL DEFAULT 0;
    PRINT '  ✓ Added IsServiceCharge';
END
ELSE
    PRINT '  - IsServiceCharge already exists';

GO

-- ============================================================================
-- PART 2: Enhance fact_payments (if table exists)
-- ============================================================================

IF OBJECT_ID('dbo.fact_payments', 'U') IS NOT NULL
BEGIN
    PRINT 'Adding new columns to fact_payments...';

    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('dbo.fact_payments') AND name = 'PaymentReference')
    BEGIN
        ALTER TABLE dbo.fact_payments
        ADD PaymentReference VARCHAR(255) NULL;
        PRINT '  ✓ Added PaymentReference';
    END
    ELSE
        PRINT '  - PaymentReference already exists';

    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('dbo.fact_payments') AND name = 'EODSessionID')
    BEGIN
        ALTER TABLE dbo.fact_payments
        ADD EODSessionID VARCHAR(50) NULL;
        PRINT '  ✓ Added EODSessionID (declaration session)';
    END
    ELSE
        PRINT '  - EODSessionID already exists';

    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('dbo.fact_payments') AND name = 'TenderAmount')
    BEGIN
        ALTER TABLE dbo.fact_payments
        ADD TenderAmount DECIMAL(18,4) NULL;
        PRINT '  ✓ Added TenderAmount';
    END
    ELSE
        PRINT '  - TenderAmount already exists';

    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('dbo.fact_payments') AND name = 'ChangeAmount')
    BEGIN
        ALTER TABLE dbo.fact_payments
        ADD ChangeAmount DECIMAL(18,4) NULL;
        PRINT '  ✓ Added ChangeAmount';
    END
    ELSE
        PRINT '  - ChangeAmount already exists';

    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('dbo.fact_payments') AND name = 'IsVoid')
    BEGIN
        ALTER TABLE dbo.fact_payments
        ADD IsVoid BIT NOT NULL DEFAULT 0;
        PRINT '  ✓ Added IsVoid';
    END
    ELSE
        PRINT '  - IsVoid already exists';
END
ELSE
    PRINT 'fact_payments table does not exist (optional for API ETL)';

GO

-- ============================================================================
-- PART 3: Create staging tables for API data
-- ============================================================================

PRINT 'Creating staging tables for API ETL...';

-- Staging: Sales Header
IF OBJECT_ID('dbo.staging_sales_api', 'U') IS NOT NULL
    DROP TABLE dbo.staging_sales_api;

CREATE TABLE dbo.staging_sales_api (
    SaleID BIGINT NOT NULL,
    BusinessDateTime DATETIME NOT NULL,
    SystemDateTime DATETIME NOT NULL,
    OutletID VARCHAR(255) NOT NULL,
    OutletName VARCHAR(255) NOT NULL,
    CashierName VARCHAR(255) NULL,
    SalesType VARCHAR(50) NOT NULL,
    SubSalesType VARCHAR(50) NULL,
    GrandTotal DECIMAL(18,4) NOT NULL,
    NetAmount DECIMAL(18,4) NOT NULL,
    TaxAmount DECIMAL(18,4) NOT NULL,
    Paid DECIMAL(18,4) NOT NULL,
    Balance DECIMAL(18,4) NOT NULL,
    Rounding DECIMAL(18,4) NULL,
    PaxNumber INT NULL,
    BillDiscountAmount DECIMAL(18,4) NULL,
    OrderNo VARCHAR(255) NULL,
    PaymentStatus VARCHAR(50) NULL,
    Status VARCHAR(50) NOT NULL,
    LoadedAt DATETIME NOT NULL DEFAULT GETDATE(),
    BatchID VARCHAR(50) NULL,
    CONSTRAINT PK_staging_sales_api PRIMARY KEY (SaleID)
);

PRINT '  ✓ Created staging_sales_api';

-- Staging: Sales Items
IF OBJECT_ID('dbo.staging_sales_items_api', 'U') IS NOT NULL
    DROP TABLE dbo.staging_sales_items_api;

CREATE TABLE dbo.staging_sales_items_api (
    ItemID BIGINT NOT NULL,
    SaleID BIGINT NOT NULL,
    ProductID INT NOT NULL,
    ProductCode VARCHAR(255) NOT NULL,
    ProductName VARCHAR(255) NOT NULL,
    Category VARCHAR(100) NULL,
    Quantity DECIMAL(10,2) NOT NULL,
    UnitPrice DECIMAL(18,4) NOT NULL,
    Subtotal DECIMAL(18,4) NOT NULL,
    DiscountAmount DECIMAL(18,4) NOT NULL,
    NetAmount DECIMAL(18,4) NOT NULL,
    TaxAmount DECIMAL(18,4) NOT NULL,
    TotalAmount DECIMAL(18,4) NOT NULL,
    TaxCode VARCHAR(10) NULL,
    TaxRate DECIMAL(5,2) NULL,
    Cost DECIMAL(18,4) NULL,
    IsFOC BIT NOT NULL DEFAULT 0,
    Model VARCHAR(100) NULL,
    IsServiceCharge BIT NOT NULL DEFAULT 0,
    SalesType VARCHAR(50) NOT NULL,
    SubSalesType VARCHAR(50) NULL,
    SalesPerson VARCHAR(255) NULL,
    LoadedAt DATETIME NOT NULL DEFAULT GETDATE(),
    BatchID VARCHAR(50) NULL,
    CONSTRAINT PK_staging_sales_items_api PRIMARY KEY (ItemID)
);

CREATE INDEX IX_staging_sales_items_api_SaleID ON dbo.staging_sales_items_api(SaleID);

PRINT '  ✓ Created staging_sales_items_api';

-- Staging: Payments
IF OBJECT_ID('dbo.staging_payments_api', 'U') IS NOT NULL
    DROP TABLE dbo.staging_payments_api;

CREATE TABLE dbo.staging_payments_api (
    PaymentID BIGINT NOT NULL,
    SaleID BIGINT NOT NULL,
    PaymentMethod VARCHAR(50) NOT NULL,
    Amount DECIMAL(18,4) NOT NULL,
    PaymentDateTime DATETIME NOT NULL,
    BusinessDate DATE NOT NULL,
    PaymentReference VARCHAR(255) NULL,
    EODSessionID VARCHAR(50) NULL,
    TenderAmount DECIMAL(18,4) NULL,
    ChangeAmount DECIMAL(18,4) NULL,
    CardType VARCHAR(100) NULL,
    IsVoid BIT NOT NULL DEFAULT 0,
    LoadedAt DATETIME NOT NULL DEFAULT GETDATE(),
    BatchID VARCHAR(50) NULL,
    CONSTRAINT PK_staging_payments_api PRIMARY KEY (PaymentID)
);

CREATE INDEX IX_staging_payments_api_SaleID ON dbo.staging_payments_api(SaleID);
CREATE INDEX IX_staging_payments_api_EODSessionID ON dbo.staging_payments_api(EODSessionID);

PRINT '  ✓ Created staging_payments_api';

GO

-- ============================================================================
-- PART 4: Create API Metadata Table (track sync progress)
-- ============================================================================

IF OBJECT_ID('dbo.api_sync_metadata', 'U') IS NOT NULL
    DROP TABLE dbo.api_sync_metadata;

CREATE TABLE dbo.api_sync_metadata (
    SyncID INT IDENTITY(1,1) PRIMARY KEY,
    LastTimestamp VARCHAR(50) NOT NULL,          -- Hex timestamp from API
    LastSyncDate DATETIME NOT NULL,              -- When sync occurred
    RecordsRetrieved INT NOT NULL,               -- Number of records in this batch
    TotalRecords INT NOT NULL,                   -- Cumulative total
    SyncStatus VARCHAR(20) NOT NULL,             -- 'Success', 'Failed', 'In Progress'
    ErrorMessage VARCHAR(MAX) NULL,
    CreatedAt DATETIME NOT NULL DEFAULT GETDATE()
);

CREATE INDEX IX_api_sync_metadata_LastSyncDate ON dbo.api_sync_metadata(LastSyncDate);

PRINT '  ✓ Created api_sync_metadata (tracks sync progress)';

GO

-- ============================================================================
-- PART 5: Create helper views for API data validation
-- ============================================================================

-- View: Compare API staging vs fact table counts
IF OBJECT_ID('dbo.vw_api_data_quality_check', 'V') IS NOT NULL
    DROP VIEW dbo.vw_api_data_quality_check;
GO

CREATE VIEW dbo.vw_api_data_quality_check AS
SELECT 
    'Sales Header' as DataType,
    COUNT(*) as StagingCount,
    (SELECT COUNT(DISTINCT SaleNumber) FROM dbo.fact_sales_transactions_api) as FactCount_API,
    (SELECT MAX(LoadedAt) FROM dbo.staging_sales_api) as LastLoadTime
FROM dbo.staging_sales_api
UNION ALL
SELECT 
    'Sales Items' as DataType,
    COUNT(*) as StagingCount,
    (SELECT COUNT(*) FROM dbo.fact_sales_transactions_api) as FactCount_API,
    (SELECT MAX(LoadedAt) FROM dbo.staging_sales_items_api) as LastLoadTime
FROM dbo.staging_sales_items_api
UNION ALL
SELECT 
    'Payments' as DataType,
    COUNT(*) as StagingCount,
    0 as FactCount_API,  -- Payments are embedded in fact_sales_transactions_api
    (SELECT MAX(LoadedAt) FROM dbo.staging_payments_api) as LastLoadTime
FROM dbo.staging_payments_api;
GO

PRINT '  ✓ Created vw_api_data_quality_check';

GO

-- ============================================================================
-- PART 6: Document new fields
-- ============================================================================

-- Extended properties for documentation
EXEC sys.sp_addextendedproperty 
    @name = N'MS_Description', 
    @value = N'Tax category code (SV=Standard, ZR=Zero-rated, SR=Special Rate)', 
    @level0type = N'SCHEMA', @level0name = 'dbo',
    @level1type = N'TABLE',  @level1name = 'fact_sales_transactions',
    @level2type = N'COLUMN', @level2name = 'TaxCode';

EXEC sys.sp_addextendedproperty 
    @name = N'MS_Description', 
    @value = N'Tax rate percentage (e.g., 6.00 for 6% GST)', 
    @level0type = N'SCHEMA', @level0name = 'dbo',
    @level1type = N'TABLE',  @level1name = 'fact_sales_transactions',
    @level2type = N'COLUMN', @level2name = 'TaxRate';

EXEC sys.sp_addextendedproperty 
    @name = N'MS_Description', 
    @value = N'Free of charge flag (1=FOC, 0=Paid). Important for promotion analysis.', 
    @level0type = N'SCHEMA', @level0name = 'dbo',
    @level1type = N'TABLE',  @level1name = 'fact_sales_transactions',
    @level2type = N'COLUMN', @level2name = 'IsFOC';

EXEC sys.sp_addextendedproperty 
    @name = N'MS_Description', 
    @value = N'Rounding adjustment applied to sale (typically ±RM 0.05)', 
    @level0type = N'SCHEMA', @level0name = 'dbo',
    @level1type = N'TABLE',  @level1name = 'fact_sales_transactions',
    @level2type = N'COLUMN', @level2name = 'Rounding';

EXEC sys.sp_addextendedproperty 
    @name = N'MS_Description', 
    @value = N'Product variant (REGULAR, LARGE, UPSIZE, etc.)', 
    @level0type = N'SCHEMA', @level0name = 'dbo',
    @level1type = N'TABLE',  @level1name = 'fact_sales_transactions',
    @level2type = N'COLUMN', @level2name = 'Model';

EXEC sys.sp_addextendedproperty 
    @name = N'MS_Description', 
    @value = N'Service charge flag (1=Service charge item, 0=Regular item)', 
    @level0type = N'SCHEMA', @level0name = 'dbo',
    @level1type = N'TABLE',  @level1name = 'fact_sales_transactions',
    @level2type = N'COLUMN', @level2name = 'IsServiceCharge';

PRINT '  ✓ Added column descriptions';

GO

PRINT '';
PRINT '========================================';
PRINT 'Schema Enhancement Complete!';
PRINT '========================================';
PRINT '';
PRINT 'Summary of Changes:';
PRINT '  ✓ 6 new columns added to fact_sales_transactions';
PRINT '  ✓ 5 new columns added to fact_payments (if exists)';
PRINT '  ✓ 3 staging tables created for API ETL';
PRINT '  ✓ 1 metadata table for sync tracking';
PRINT '  ✓ 1 data quality check view';
PRINT '';
PRINT 'Next Steps:';
PRINT '  1. Update ETL scripts to populate new columns';
PRINT '  2. Build API extraction script (etl_from_api.py)';
PRINT '  3. Test with small date range first';
PRINT '  4. Validate data quality';
PRINT '';

