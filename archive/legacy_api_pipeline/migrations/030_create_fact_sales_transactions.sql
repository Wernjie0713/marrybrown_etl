/*
Create fact_sales_transactions with API-specific fields and supporting indexes
Author: Repo Cleanup Automation
Date: November 18, 2025
*/

USE MarryBrown_DW;
GO

PRINT 'Creating fact_sales_transactions...';
GO

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'fact_sales_transactions')
BEGIN
    CREATE TABLE dbo.fact_sales_transactions (
        TransactionKey BIGINT IDENTITY(1,1) PRIMARY KEY,
        DateKey INT,
        TimeKey INT,
        LocationKey INT,
        ProductKey INT,
        CustomerKey INT,
        StaffKey INT,
        PromotionKey INT,
        PaymentTypeKey INT,
        TerminalKey INT,
        SaleNumber NVARCHAR(45),
        SaleType NVARCHAR(50),
        SubSalesType NVARCHAR(50),
        SalesStatus NVARCHAR(20),
        OrderSource NVARCHAR(50),
        Quantity DECIMAL(18,3),
        GrossAmount DECIMAL(18,2),
        DiscountAmount DECIMAL(18,2),
        NetAmount DECIMAL(18,2),
        TaxAmount DECIMAL(18,2),
        TotalAmount DECIMAL(18,2),
        CostAmount DECIMAL(18,2),
        CardType NVARCHAR(50),
        TaxCode NVARCHAR(20),
        TaxRate DECIMAL(18,4),
        IsFOC BIT,
        Rounding DECIMAL(18,2),
        Model NVARCHAR(100),
        IsServiceCharge BIT,
        CreatedAt DATETIME DEFAULT GETDATE(),
        UpdatedAt DATETIME DEFAULT GETDATE()
    );

    CREATE NONCLUSTERED INDEX IX_fact_sales_DateKey ON dbo.fact_sales_transactions(DateKey);
    CREATE NONCLUSTERED INDEX IX_fact_sales_LocationKey ON dbo.fact_sales_transactions(LocationKey);
    CREATE NONCLUSTERED INDEX IX_fact_sales_ProductKey ON dbo.fact_sales_transactions(ProductKey);
    CREATE NONCLUSTERED INDEX IX_fact_sales_SaleNumber ON dbo.fact_sales_transactions(SaleNumber);
    CREATE NONCLUSTERED INDEX IX_fact_sales_SalesStatus ON dbo.fact_sales_transactions(SalesStatus);

    PRINT '  [OK] Created fact_sales_transactions and indexes';
END
ELSE PRINT '  - fact_sales_transactions already exists';
GO

