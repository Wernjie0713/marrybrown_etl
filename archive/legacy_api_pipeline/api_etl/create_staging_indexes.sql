/*
 * Staging Table Indexes for Performance Optimization
 * 
 * These indexes significantly improve performance for:
 * - Transform phase joins (staging_sales -> staging_sales_items -> staging_payments)
 * - MERGE operations in transform_to_facts_optimized()
 * - Chunked processing queries
 * 
 * Run this script on your MarryBrown_DW database before running ETL.
 * 
 * Author: YONG WERN JIE
 * Date: December 2025
 */

USE MarryBrown_DW;
GO

-- Index on staging_sales.SaleID (primary join key)
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_staging_sales_SaleID' AND object_id = OBJECT_ID('dbo.staging_sales'))
BEGIN
    CREATE NONCLUSTERED INDEX IX_staging_sales_SaleID 
    ON dbo.staging_sales(SaleID);
    PRINT 'Created index: IX_staging_sales_SaleID';
END
ELSE
    PRINT 'Index already exists: IX_staging_sales_SaleID';
GO

-- Index on staging_sales.BusinessDateTime (for date filtering and ordering)
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_staging_sales_BusinessDateTime' AND object_id = OBJECT_ID('dbo.staging_sales'))
BEGIN
    CREATE NONCLUSTERED INDEX IX_staging_sales_BusinessDateTime 
    ON dbo.staging_sales(BusinessDateTime);
    PRINT 'Created index: IX_staging_sales_BusinessDateTime';
END
ELSE
    PRINT 'Index already exists: IX_staging_sales_BusinessDateTime';
GO

-- Index on staging_sales.OutletName (for dimension join)
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_staging_sales_OutletName' AND object_id = OBJECT_ID('dbo.staging_sales'))
BEGIN
    CREATE NONCLUSTERED INDEX IX_staging_sales_OutletName 
    ON dbo.staging_sales(OutletName);
    PRINT 'Created index: IX_staging_sales_OutletName';
END
ELSE
    PRINT 'Index already exists: IX_staging_sales_OutletName';
GO

-- Index on staging_sales_items.SaleID (primary join key)
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_staging_sales_items_SaleID' AND object_id = OBJECT_ID('dbo.staging_sales_items'))
BEGIN
    CREATE NONCLUSTERED INDEX IX_staging_sales_items_SaleID 
    ON dbo.staging_sales_items(SaleID);
    PRINT 'Created index: IX_staging_sales_items_SaleID';
END
ELSE
    PRINT 'Index already exists: IX_staging_sales_items_SaleID';
GO

-- Index on staging_sales_items.ProductID (for dimension join)
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_staging_sales_items_ProductID' AND object_id = OBJECT_ID('dbo.staging_sales_items'))
BEGIN
    CREATE NONCLUSTERED INDEX IX_staging_sales_items_ProductID 
    ON dbo.staging_sales_items(ProductID);
    PRINT 'Created index: IX_staging_sales_items_ProductID';
END
ELSE
    PRINT 'Index already exists: IX_staging_sales_items_ProductID';
GO

-- Index on staging_payments.SaleID (primary join key)
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_staging_payments_SaleID' AND object_id = OBJECT_ID('dbo.staging_payments'))
BEGIN
    CREATE NONCLUSTERED INDEX IX_staging_payments_SaleID 
    ON dbo.staging_payments(SaleID);
    PRINT 'Created index: IX_staging_payments_SaleID';
END
ELSE
    PRINT 'Index already exists: IX_staging_payments_SaleID';
GO

-- Index on staging_payments.PaymentMethod (for dimension join)
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_staging_payments_PaymentMethod' AND object_id = OBJECT_ID('dbo.staging_payments'))
BEGIN
    CREATE NONCLUSTERED INDEX IX_staging_payments_PaymentMethod 
    ON dbo.staging_payments(PaymentMethod);
    PRINT 'Created index: IX_staging_payments_PaymentMethod';
END
ELSE
    PRINT 'Index already exists: IX_staging_payments_PaymentMethod';
GO

-- Composite index for void check in PaymentAllocations CTE
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_staging_payments_SaleID_IsVoid' AND object_id = OBJECT_ID('dbo.staging_payments'))
BEGIN
    CREATE NONCLUSTERED INDEX IX_staging_payments_SaleID_IsVoid 
    ON dbo.staging_payments(SaleID, IsVoid);
    PRINT 'Created index: IX_staging_payments_SaleID_IsVoid';
END
ELSE
    PRINT 'Index already exists: IX_staging_payments_SaleID_IsVoid';
GO

PRINT '';
PRINT 'All staging table indexes created successfully!';
PRINT 'These indexes will improve transform performance significantly.';
GO

