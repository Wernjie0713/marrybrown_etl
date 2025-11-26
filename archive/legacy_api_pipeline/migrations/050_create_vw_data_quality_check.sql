/*
Create vw_data_quality_check to summarize ingestion freshness
Author: Repo Cleanup Automation
Date: November 18, 2025
*/

USE MarryBrown_DW;
GO

PRINT 'Creating vw_data_quality_check...';
GO

IF EXISTS (SELECT * FROM sys.views WHERE name = 'vw_data_quality_check')
BEGIN
    DROP VIEW dbo.vw_data_quality_check;
    PRINT '  - Dropped existing vw_data_quality_check';
END
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
    0 as FactCount,
    (SELECT MAX(LoadedAt) FROM dbo.staging_payments) as LastLoadTime
FROM dbo.staging_payments;
GO

PRINT 'vw_data_quality_check ready.';
GO

