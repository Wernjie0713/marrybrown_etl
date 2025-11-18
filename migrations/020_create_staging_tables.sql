/*
Create API-aligned staging tables with wide character columns for resume-friendly loads
Author: Repo Cleanup Automation
Date: November 18, 2025
*/

USE MarryBrown_DW;
GO

PRINT 'Creating staging tables...';
GO

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'staging_sales')
BEGIN
    CREATE TABLE dbo.staging_sales (
        SaleID NVARCHAR(100),
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
        Status NVARCHAR(100),
        BatchID NVARCHAR(100),
        LoadedAt DATETIME DEFAULT GETDATE()
    );
    PRINT '  [OK] Created staging_sales';
END
ELSE PRINT '  - staging_sales already exists';

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'staging_sales_items')
BEGIN
    CREATE TABLE dbo.staging_sales_items (
        ItemID NVARCHAR(100),
        SaleID NVARCHAR(100),
        ProductID NVARCHAR(100),
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
        TaxCode NVARCHAR(100),
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
END
ELSE PRINT '  - staging_sales_items already exists';

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'staging_payments')
BEGIN
    CREATE TABLE dbo.staging_payments (
        PaymentID NVARCHAR(100),
        SaleID NVARCHAR(100),
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
END
ELSE PRINT '  - staging_payments already exists';

PRINT 'Staging table creation complete.';
GO

