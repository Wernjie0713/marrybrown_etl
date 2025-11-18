-- Fix ID columns from BIGINT to NVARCHAR
-- Xilnex uses MongoDB ObjectId strings (24 hex chars) like "5c6f3ab8e2bd980001e615c9"

USE MarryBrown_DW;
GO

PRINT '=== Fixing ID Columns in Staging Tables ===';
GO

-- staging_sales
ALTER TABLE dbo.staging_sales
    ALTER COLUMN SaleID NVARCHAR(50);
PRINT '  [OK] staging_sales.SaleID: BIGINT -> NVARCHAR(50)';
GO

-- staging_sales_items  
ALTER TABLE dbo.staging_sales_items
    ALTER COLUMN ItemID NVARCHAR(50);
PRINT '  [OK] staging_sales_items.ItemID: BIGINT -> NVARCHAR(50)';

ALTER TABLE dbo.staging_sales_items
    ALTER COLUMN SaleID NVARCHAR(50);
PRINT '  [OK] staging_sales_items.SaleID: BIGINT -> NVARCHAR(50)';

ALTER TABLE dbo.staging_sales_items
    ALTER COLUMN ProductID NVARCHAR(50);
PRINT '  [OK] staging_sales_items.ProductID: existing NVARCHAR';
GO

-- staging_payments
ALTER TABLE dbo.staging_payments
    ALTER COLUMN PaymentID NVARCHAR(50);
PRINT '  [OK] staging_payments.PaymentID: BIGINT -> NVARCHAR(50)';

ALTER TABLE dbo.staging_payments
    ALTER COLUMN SaleID NVARCHAR(50);
PRINT '  [OK] staging_payments.SaleID: BIGINT -> NVARCHAR(50)';
GO

PRINT '';
PRINT '=== Verifying ID Columns ===';
GO

SELECT 
    'staging_sales' AS TableName,
    c.name AS ColumnName,
    t.name AS DataType,
    CASE 
        WHEN t.name LIKE 'n%char' THEN c.max_length / 2 
        ELSE c.max_length 
    END AS Size
FROM sys.columns c
INNER JOIN sys.types t ON c.user_type_id = t.user_type_id
WHERE c.object_id = OBJECT_ID('dbo.staging_sales')
    AND c.name LIKE '%ID'
UNION ALL
SELECT 
    'staging_sales_items' AS TableName,
    c.name AS ColumnName,
    t.name AS DataType,
    CASE 
        WHEN t.name LIKE 'n%char' THEN c.max_length / 2 
        ELSE c.max_length 
    END AS Size
FROM sys.columns c
INNER JOIN sys.types t ON c.user_type_id = t.user_type_id
WHERE c.object_id = OBJECT_ID('dbo.staging_sales_items')
    AND c.name LIKE '%ID'
UNION ALL
SELECT 
    'staging_payments' AS TableName,
    c.name AS ColumnName,
    t.name AS DataType,
    CASE 
        WHEN t.name LIKE 'n%char' THEN c.max_length / 2 
        ELSE c.max_length 
    END AS Size
FROM sys.columns c
INNER JOIN sys.types t ON c.user_type_id = t.user_type_id
WHERE c.object_id = OBJECT_ID('dbo.staging_payments')
    AND c.name LIKE '%ID'
ORDER BY TableName, ColumnName;
GO

PRINT '';
PRINT '=== ID Columns Fixed Successfully! ===';
GO

