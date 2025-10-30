-- Fix column size issue in staging_sales
-- Error: "String data, right truncation: length 48 buffer 46"
-- Some columns are too small for the data

USE MarryBrown_DW;
GO

PRINT '=== Checking Current Column Sizes ===';
GO

SELECT 
    c.name AS ColumnName,
    t.name AS DataType,
    c.max_length AS MaxLength,
    CASE 
        WHEN t.name LIKE 'n%char' THEN c.max_length / 2 
        ELSE c.max_length 
    END AS ActualCharLength
FROM sys.columns c
INNER JOIN sys.types t ON c.user_type_id = t.user_type_id
WHERE c.object_id = OBJECT_ID('dbo.staging_sales')
    AND t.name LIKE '%char%'
ORDER BY c.column_id;
GO

PRINT '';
PRINT '=== Increasing Column Sizes ===';
GO

-- Fix all VARCHAR/NVARCHAR columns in staging_sales
ALTER TABLE dbo.staging_sales
    ALTER COLUMN OutletID NVARCHAR(100);
PRINT '  [OK] OutletID: 50 -> 100';

ALTER TABLE dbo.staging_sales
    ALTER COLUMN OutletName NVARCHAR(300);
PRINT '  [OK] OutletName: 200 -> 300';

ALTER TABLE dbo.staging_sales
    ALTER COLUMN CashierName NVARCHAR(300);
PRINT '  [OK] CashierName: 200 -> 300';

ALTER TABLE dbo.staging_sales
    ALTER COLUMN SalesType NVARCHAR(100);
PRINT '  [OK] SalesType: 50 -> 100';

ALTER TABLE dbo.staging_sales
    ALTER COLUMN SubSalesType NVARCHAR(100);
PRINT '  [OK] SubSalesType: 50 -> 100';

ALTER TABLE dbo.staging_sales
    ALTER COLUMN OrderNo NVARCHAR(100);
PRINT '  [OK] OrderNo: 50 -> 100';

ALTER TABLE dbo.staging_sales
    ALTER COLUMN PaymentStatus NVARCHAR(100);
PRINT '  [OK] PaymentStatus: 50 -> 100';

ALTER TABLE dbo.staging_sales
    ALTER COLUMN Status NVARCHAR(50);
PRINT '  [OK] Status: 20 -> 50';

ALTER TABLE dbo.staging_sales
    ALTER COLUMN BatchID NVARCHAR(100);
PRINT '  [OK] BatchID: 50 -> 100';

GO

PRINT '';
PRINT '=== Verifying New Sizes ===';
GO

SELECT 
    c.name AS ColumnName,
    t.name AS DataType,
    CASE 
        WHEN t.name LIKE 'n%char' THEN c.max_length / 2 
        ELSE c.max_length 
    END AS NewCharLength
FROM sys.columns c
INNER JOIN sys.types t ON c.user_type_id = t.user_type_id
WHERE c.object_id = OBJECT_ID('dbo.staging_sales')
    AND t.name LIKE '%char%'
ORDER BY c.column_id;
GO

PRINT '';
PRINT '=== Column Sizes Fixed Successfully! ===';
GO

