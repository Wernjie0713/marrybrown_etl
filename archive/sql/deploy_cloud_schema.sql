/*
=============================================================================
CLOUD WAREHOUSE SCHEMA DEPLOYMENT
Deploy full MarryBrown data warehouse schema to TIMEdotcom cloud
Includes: Dimension tables, Staging tables, Fact tables, Metadata tables
Author: YONG WERN JIE
Date: October 29, 2025
=============================================================================
*/

USE MarryBrown_DW;
GO

PRINT '========================================';
PRINT 'CLOUD WAREHOUSE SCHEMA DEPLOYMENT';
PRINT '========================================';
PRINT '';

-- =============================================================================
-- PART 1: DIMENSION TABLES
-- =============================================================================
PRINT 'PART 1: Creating Dimension Tables...';
PRINT '';

-- dim_date
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'dim_date')
BEGIN
    CREATE TABLE dbo.dim_date (
        DateKey INT PRIMARY KEY,
        FullDate DATE NOT NULL,
        DayOfWeek INT,
        DayName VARCHAR(10),
        DayOfMonth INT,
        DayOfYear INT,
        WeekOfYear INT,
        MonthName VARCHAR(10),
        MonthOfYear INT,
        Quarter INT,
        Year INT,
        IsWeekend BIT,
        IsHoliday BIT DEFAULT 0
    );
    PRINT '  [OK] Created dim_date';
END
ELSE
    PRINT '  - dim_date already exists';

-- dim_time
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'dim_time')
BEGIN
    CREATE TABLE dbo.dim_time (
        TimeKey INT PRIMARY KEY,
        FullTime TIME NOT NULL,
        Hour INT,
        Minute INT,
        Second INT,
        HourName VARCHAR(10),
        PeriodOfDay VARCHAR(20)
    );
    PRINT '  [OK] Created dim_time';
END
ELSE
    PRINT '  - dim_time already exists';

-- dim_locations
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'dim_locations')
BEGIN
    CREATE TABLE dbo.dim_locations (
        LocationKey INT IDENTITY(1,1) PRIMARY KEY,
        LocationGUID NVARCHAR(50) NOT NULL,  -- Matches ETL script expectation
        LocationName NVARCHAR(200),
        City NVARCHAR(100),
        State NVARCHAR(100),
        IsActive BIT DEFAULT 1
    );
    PRINT '  [OK] Created dim_locations';
END
ELSE
    PRINT '  - dim_locations already exists';

-- dim_products (matches etl_dim_products.py)
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'dim_products')
BEGIN
    CREATE TABLE dbo.dim_products (
        ProductKey INT IDENTITY(1,1) PRIMARY KEY,
        SourceProductID NVARCHAR(50) NOT NULL,
        ProductCode NVARCHAR(50),
        ProductName NVARCHAR(200),
        Category NVARCHAR(100),
        ProductType NVARCHAR(50),
        Brand NVARCHAR(100),
        CurrentSalePrice DECIMAL(18,2),
        IsPackage BIT DEFAULT 0,
        IsActive BIT DEFAULT 1
    );
    PRINT '  [OK] Created dim_products';
END
ELSE
    PRINT '  - dim_products already exists';

-- dim_customers (matches etl_dim_customers.py)
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'dim_customers')
BEGIN
    CREATE TABLE dbo.dim_customers (
        CustomerKey INT IDENTITY(1,1) PRIMARY KEY,
        CustomerGUID NVARCHAR(50) NOT NULL,
        CustomerCode NVARCHAR(500),  -- Increased from 50 to 500 for JWT tokens
        FullName NVARCHAR(500),  -- Increased from 200 to 500 for long names
        FirstName NVARCHAR(500),  -- Increased from 100 to 500 for long names
        LastName NVARCHAR(500),  -- Increased from 100 to 500 for garbage data
        MobileNumber NVARCHAR(50),
        Email NVARCHAR(200),
        CustomerGroup NVARCHAR(100),
        CurrentLoyaltyPoints DECIMAL(18,2),
        RegistrationDate DATE,
        DateOfBirth DATE,
        IsActive BIT DEFAULT 1
    );
    PRINT '  [OK] Created dim_customers';
END
ELSE
    PRINT '  - dim_customers already exists';

-- dim_staff (matches etl_dim_staff.py)
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'dim_staff')
BEGIN
    CREATE TABLE dbo.dim_staff (
        StaffKey INT IDENTITY(1,1) PRIMARY KEY,
        StaffUsername NVARCHAR(200) NOT NULL,
        StaffFullName NVARCHAR(200),
        StaffType NVARCHAR(100)
    );
    PRINT '  [OK] Created dim_staff';
END
ELSE
    PRINT '  - dim_staff already exists';

-- dim_payment_types (matches etl_dim_payment_types.py)
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'dim_payment_types')
BEGIN
    CREATE TABLE dbo.dim_payment_types (
        PaymentTypeKey INT IDENTITY(1,1) PRIMARY KEY,
        PaymentMethodName NVARCHAR(200) NOT NULL,
        PaymentCategory NVARCHAR(50)
    );
    PRINT '  [OK] Created dim_payment_types';
END
ELSE
    PRINT '  - dim_payment_types already exists';

-- dim_promotions (matches etl_dim_promotions.py)
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'dim_promotions')
BEGIN
    CREATE TABLE dbo.dim_promotions (
        PromotionKey INT IDENTITY(1,1) PRIMARY KEY,
        SourcePromotionID NVARCHAR(50) NOT NULL,
        PromotionName NVARCHAR(200),
        PromotionDescription NVARCHAR(500),
        PromotionCode NVARCHAR(200),  -- Increased from 50 to 200 for long promo codes
        PromotionType NVARCHAR(50),
        StartDate DATE,
        EndDate DATE,
        IsActive BIT DEFAULT 1
    );
    PRINT '  [OK] Created dim_promotions';
END
ELSE
    PRINT '  - dim_promotions already exists';

-- dim_terminals (matches etl_dim_terminals.py)
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'dim_terminals')
BEGIN
    CREATE TABLE dbo.dim_terminals (
        TerminalKey INT IDENTITY(1,1) PRIMARY KEY,
        TerminalID NVARCHAR(50) NOT NULL,
        LocationKey INT,
        TerminalName NVARCHAR(100),
        IsActive BIT DEFAULT 1,
        FOREIGN KEY (LocationKey) REFERENCES dbo.dim_locations(LocationKey)
    );
    PRINT '  [OK] Created dim_terminals';
END
ELSE
    PRINT '  - dim_terminals already exists';

PRINT '';
PRINT 'PART 1 COMPLETE: All dimension tables ready';
PRINT '';

-- =============================================================================
-- PART 2: STAGING TABLES (API ETL)
-- =============================================================================
PRINT 'PART 2: Creating Staging Tables...';
PRINT '';

-- staging_sales (matches API ETL structure)
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'staging_sales')
BEGIN
    CREATE TABLE dbo.staging_sales (
        SaleID BIGINT,
        BusinessDateTime DATETIME,
        SystemDateTime DATETIME,
        OutletID NVARCHAR(50),
        OutletName NVARCHAR(200),
        CashierName NVARCHAR(200),
        SalesType NVARCHAR(50),
        SubSalesType NVARCHAR(50),
        GrandTotal DECIMAL(18,2),
        NetAmount DECIMAL(18,2),
        TaxAmount DECIMAL(18,2),
        Paid DECIMAL(18,2),
        Balance DECIMAL(18,2),
        Rounding DECIMAL(18,2),
        PaxNumber INT,
        BillDiscountAmount DECIMAL(18,2),
        OrderNo NVARCHAR(50),
        PaymentStatus NVARCHAR(50),
        Status NVARCHAR(20),
        BatchID NVARCHAR(50),
        LoadedAt DATETIME DEFAULT GETDATE()
    );
    PRINT '  [OK] Created staging_sales';
END
ELSE
    PRINT '  - staging_sales already exists';

-- staging_sales_items (matches API ETL structure)
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'staging_sales_items')
BEGIN
    CREATE TABLE dbo.staging_sales_items (
        ItemID BIGINT,
        SaleID BIGINT,
        ProductID NVARCHAR(50),
        ProductCode NVARCHAR(50),
        ProductName NVARCHAR(200),
        Category NVARCHAR(100),
        Quantity DECIMAL(18,3),
        UnitPrice DECIMAL(18,2),
        Subtotal DECIMAL(18,2),
        DiscountAmount DECIMAL(18,2),
        NetAmount DECIMAL(18,2),
        TaxAmount DECIMAL(18,2),
        TotalAmount DECIMAL(18,2),
        TaxCode NVARCHAR(20),
        TaxRate DECIMAL(18,4),
        Cost DECIMAL(18,2),
        IsFOC BIT,
        Model NVARCHAR(100),
        IsServiceCharge BIT,
        SalesType NVARCHAR(50),
        SubSalesType NVARCHAR(50),
        SalesPerson NVARCHAR(200),
        BatchID NVARCHAR(50),
        LoadedAt DATETIME DEFAULT GETDATE()
    );
    PRINT '  [OK] Created staging_sales_items';
END
ELSE
    PRINT '  - staging_sales_items already exists';

-- staging_payments (matches API ETL structure)
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'staging_payments')
BEGIN
    CREATE TABLE dbo.staging_payments (
        PaymentID BIGINT,
        SaleID BIGINT,
        PaymentMethod NVARCHAR(100),
        Amount DECIMAL(18,2),
        PaymentDateTime DATETIME,
        BusinessDate DATE,
        PaymentReference NVARCHAR(100),
        EODSessionID NVARCHAR(50),
        TenderAmount DECIMAL(18,2),
        ChangeAmount DECIMAL(18,2),
        CardType NVARCHAR(50),
        IsVoid BIT,
        BatchID NVARCHAR(50),
        LoadedAt DATETIME DEFAULT GETDATE()
    );
    PRINT '  [OK] Created staging_payments';
END
ELSE
    PRINT '  - staging_payments already exists';

PRINT '';
PRINT 'PART 2 COMPLETE: All staging tables ready';
PRINT '';

-- =============================================================================
-- PART 3: FACT TABLE
-- =============================================================================
PRINT 'PART 3: Creating Fact Table...';
PRINT '';

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'fact_sales_transactions')
BEGIN
    CREATE TABLE dbo.fact_sales_transactions (
        TransactionKey BIGINT IDENTITY(1,1) PRIMARY KEY,
        
        -- Date and Time Dimensions
        DateKey INT,
        TimeKey INT,
        
        -- Dimension Keys
        LocationKey INT,
        ProductKey INT,
        CustomerKey INT,
        StaffKey INT,
        PromotionKey INT,
        PaymentTypeKey INT,
        TerminalKey INT,
        
        -- Transaction Identifiers
        SaleNumber NVARCHAR(45),
        SaleType NVARCHAR(50),
        SubSalesType NVARCHAR(50),
        SalesStatus NVARCHAR(20),
        OrderSource NVARCHAR(50),
        
        -- Quantity Measures
        Quantity DECIMAL(18, 3),
        
        -- Amount Measures
        GrossAmount DECIMAL(18, 2),
        DiscountAmount DECIMAL(18, 2),
        NetAmount DECIMAL(18, 2),
        TaxAmount DECIMAL(18, 2),
        TotalAmount DECIMAL(18, 2),
        CostAmount DECIMAL(18, 2),
        
        -- Payment Details
        CardType NVARCHAR(50),
        
        -- NEW API-SPECIFIC FIELDS
        TaxCode NVARCHAR(20),
        TaxRate DECIMAL(18, 4),
        IsFOC BIT,
        Rounding DECIMAL(18, 2),
        Model NVARCHAR(100),
        IsServiceCharge BIT,
        
        -- Audit Fields
        CreatedAt DATETIME DEFAULT GETDATE(),
        UpdatedAt DATETIME DEFAULT GETDATE()
    );
    
    -- Create indexes for performance
    CREATE NONCLUSTERED INDEX IX_fact_sales_DateKey ON dbo.fact_sales_transactions(DateKey);
    CREATE NONCLUSTERED INDEX IX_fact_sales_LocationKey ON dbo.fact_sales_transactions(LocationKey);
    CREATE NONCLUSTERED INDEX IX_fact_sales_ProductKey ON dbo.fact_sales_transactions(ProductKey);
    CREATE NONCLUSTERED INDEX IX_fact_sales_SaleNumber ON dbo.fact_sales_transactions(SaleNumber);
    CREATE NONCLUSTERED INDEX IX_fact_sales_SalesStatus ON dbo.fact_sales_transactions(SalesStatus);
    
    PRINT '  [OK] Created fact_sales_transactions with 6 new API fields';
    PRINT '  [OK] Created performance indexes';
END
ELSE
    PRINT '  - fact_sales_transactions already exists';

PRINT '';
PRINT 'PART 3 COMPLETE: Fact table ready';
PRINT '';

-- =============================================================================
-- PART 4: METADATA TABLES
-- =============================================================================
PRINT 'PART 4: Creating Metadata Tables...';
PRINT '';

-- api_sync_metadata
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'api_sync_metadata')
BEGIN
    CREATE TABLE dbo.api_sync_metadata (
        SyncID INT IDENTITY(1,1) PRIMARY KEY,
        LastTimestamp NVARCHAR(50),
        SyncStartTime DATETIME,
        SyncEndTime DATETIME,
        RecordsExtracted INT,
        Status NVARCHAR(20),
        ErrorMessage NVARCHAR(MAX),
        DateRangeStart DATE,
        DateRangeEnd DATE
    );
    PRINT '  [OK] Created api_sync_metadata';
END
ELSE
    PRINT '  - api_sync_metadata already exists';

PRINT '';
PRINT 'PART 4 COMPLETE: Metadata tracking ready';
PRINT '';

-- =============================================================================
-- PART 5: DATA QUALITY VIEW
-- =============================================================================
PRINT 'PART 5: Creating Data Quality View...';
PRINT '';

IF EXISTS (SELECT * FROM sys.views WHERE name = 'vw_data_quality_check')
    DROP VIEW dbo.vw_data_quality_check;
GO

CREATE VIEW dbo.vw_data_quality_check AS
SELECT 
    'Sales Header' as DataType,
    COUNT(*) as StagingCount,
    (SELECT COUNT(DISTINCT SaleNumber) FROM dbo.fact_sales_transactions) as FactCount,
    (SELECT MAX(LoadedAt) FROM dbo.staging_sales) as LastLoadTime
FROM dbo.staging_sales
UNION ALL
SELECT 
    'Sales Items' as DataType,
    COUNT(*) as StagingCount,
    (SELECT COUNT(*) FROM dbo.fact_sales_transactions) as FactCount,
    (SELECT MAX(LoadedAt) FROM dbo.staging_sales_items) as LastLoadTime
FROM dbo.staging_sales_items
UNION ALL
SELECT 
    'Payments' as DataType,
    COUNT(*) as StagingCount,
    0 as FactCount,  -- Payments are embedded in fact_sales_transactions
    (SELECT MAX(LoadedAt) FROM dbo.staging_payments) as LastLoadTime
FROM dbo.staging_payments;
GO

PRINT '  [OK] Created vw_data_quality_check';
PRINT '';

-- =============================================================================
-- VERIFICATION
-- =============================================================================
PRINT '========================================';
PRINT 'DEPLOYMENT VERIFICATION';
PRINT '========================================';
PRINT '';

-- Count tables created
SELECT 
    'Dimension Tables' as TableType,
    COUNT(*) as TableCount
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'dbo' 
  AND TABLE_NAME LIKE 'dim_%'
UNION ALL
SELECT 
    'Staging Tables',
    COUNT(*)
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'dbo' 
  AND TABLE_NAME LIKE 'staging_%'
UNION ALL
SELECT 
    'Fact Tables',
    COUNT(*)
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'dbo' 
  AND TABLE_NAME LIKE 'fact_%'
UNION ALL
SELECT 
    'Metadata Tables',
    COUNT(*)
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'dbo' 
  AND TABLE_NAME LIKE '%metadata%';

PRINT '';
PRINT '========================================';
PRINT 'CLOUD SCHEMA DEPLOYMENT COMPLETE!';
PRINT '========================================';
PRINT '';
PRINT 'Next Steps:';
PRINT '  1. Populate dimension tables (run etl_dim_*.py scripts)';
PRINT '  2. Run API ETL to extract Oct 2018 - Dec 2019 data';
PRINT '  3. Verify data quality using vw_data_quality_check';
PRINT '';

