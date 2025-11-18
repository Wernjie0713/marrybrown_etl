-- ============================================================================
-- FINAL FIX: Increase ALL NVARCHAR columns to 500 characters
-- ============================================================================
-- This script will find and fix ANY column that is smaller than 500 chars
-- ============================================================================

USE MarryBrown_DW;
GO

PRINT '============================================================================';
PRINT 'FINAL FIX: INCREASING ALL NVARCHAR COLUMNS TO 500 CHARACTERS';
PRINT '============================================================================';
PRINT '';

-- ============================================================================
-- PART 1: staging_sales
-- ============================================================================
PRINT 'PART 1: Fixing staging_sales table...';
PRINT '';

DECLARE @sql NVARCHAR(MAX) = '';

SELECT @sql = @sql + 
    'ALTER TABLE dbo.staging_sales ALTER COLUMN ' + c.name + ' NVARCHAR(500);' + CHAR(13)
FROM sys.columns c
INNER JOIN sys.types t ON c.user_type_id = t.user_type_id
WHERE c.object_id = OBJECT_ID('dbo.staging_sales')
    AND t.name LIKE 'n%char'
    AND c.max_length < 1000;  -- Less than 500 chars

IF @sql <> ''
BEGIN
    PRINT 'Executing ALTER statements for staging_sales:';
    PRINT @sql;
    EXEC sp_executesql @sql;
    PRINT '[OK] All staging_sales NVARCHAR columns increased to 500';
END
ELSE
BEGIN
    PRINT '[INFO] All staging_sales NVARCHAR columns already 500 or larger';
END

GO

-- ============================================================================
-- PART 2: staging_sales_items
-- ============================================================================
PRINT '';
PRINT 'PART 2: Fixing staging_sales_items table...';
PRINT '';

DECLARE @sql NVARCHAR(MAX) = '';

SELECT @sql = @sql + 
    'ALTER TABLE dbo.staging_sales_items ALTER COLUMN ' + c.name + ' NVARCHAR(500);' + CHAR(13)
FROM sys.columns c
INNER JOIN sys.types t ON c.user_type_id = t.user_type_id
WHERE c.object_id = OBJECT_ID('dbo.staging_sales_items')
    AND t.name LIKE 'n%char'
    AND c.max_length < 1000;  -- Less than 500 chars

IF @sql <> ''
BEGIN
    PRINT 'Executing ALTER statements for staging_sales_items:';
    PRINT @sql;
    EXEC sp_executesql @sql;
    PRINT '[OK] All staging_sales_items NVARCHAR columns increased to 500';
END
ELSE
BEGIN
    PRINT '[INFO] All staging_sales_items NVARCHAR columns already 500 or larger';
END

GO

-- ============================================================================
-- PART 3: staging_payments
-- ============================================================================
PRINT '';
PRINT 'PART 3: Fixing staging_payments table...';
PRINT '';

DECLARE @sql NVARCHAR(MAX) = '';

SELECT @sql = @sql + 
    'ALTER TABLE dbo.staging_payments ALTER COLUMN ' + c.name + ' NVARCHAR(500);' + CHAR(13)
FROM sys.columns c
INNER JOIN sys.types t ON c.user_type_id = t.user_type_id
WHERE c.object_id = OBJECT_ID('dbo.staging_payments')
    AND t.name LIKE 'n%char'
    AND c.max_length < 1000;  -- Less than 500 chars

IF @sql <> ''
BEGIN
    PRINT 'Executing ALTER statements for staging_payments:';
    PRINT @sql;
    EXEC sp_executesql @sql;
    PRINT '[OK] All staging_payments NVARCHAR columns increased to 500';
END
ELSE
BEGIN
    PRINT '[INFO] All staging_payments NVARCHAR columns already 500 or larger';
END

GO

-- ============================================================================
-- VERIFICATION
-- ============================================================================
PRINT '';
PRINT '============================================================================';
PRINT 'VERIFICATION: All column sizes after fix';
PRINT '============================================================================';
PRINT '';

PRINT 'staging_sales:';
SELECT 
    c.name AS ColumnName,
    t.name AS DataType,
    CASE 
        WHEN t.name LIKE 'n%char' THEN 
            CASE 
                WHEN c.max_length = -1 THEN 'MAX'
                ELSE CAST(c.max_length / 2 AS VARCHAR(10))
            END
        WHEN t.name LIKE '%char' THEN CAST(c.max_length AS VARCHAR(10))
        ELSE 'N/A'
    END AS CharLength
FROM sys.columns c
INNER JOIN sys.types t ON c.user_type_id = t.user_type_id
WHERE c.object_id = OBJECT_ID('dbo.staging_sales')
    AND t.name LIKE '%char'
ORDER BY c.column_id;

GO

PRINT '';
PRINT 'staging_sales_items:';
SELECT 
    c.name AS ColumnName,
    t.name AS DataType,
    CASE 
        WHEN t.name LIKE 'n%char' THEN 
            CASE 
                WHEN c.max_length = -1 THEN 'MAX'
                ELSE CAST(c.max_length / 2 AS VARCHAR(10))
            END
        WHEN t.name LIKE '%char' THEN CAST(c.max_length AS VARCHAR(10))
        ELSE 'N/A'
    END AS CharLength
FROM sys.columns c
INNER JOIN sys.types t ON c.user_type_id = t.user_type_id
WHERE c.object_id = OBJECT_ID('dbo.staging_sales_items')
    AND t.name LIKE '%char'
ORDER BY c.column_id;

GO

PRINT '';
PRINT 'staging_payments:';
SELECT 
    c.name AS ColumnName,
    t.name AS DataType,
    CASE 
        WHEN t.name LIKE 'n%char' THEN 
            CASE 
                WHEN c.max_length = -1 THEN 'MAX'
                ELSE CAST(c.max_length / 2 AS VARCHAR(10))
            END
        WHEN t.name LIKE '%char' THEN CAST(c.max_length AS VARCHAR(10))
        ELSE 'N/A'
    END AS CharLength
FROM sys.columns c
INNER JOIN sys.types t ON c.user_type_id = t.user_type_id
WHERE c.object_id = OBJECT_ID('dbo.staging_payments')
    AND t.name LIKE '%char'
ORDER BY c.column_id;

GO

PRINT '';
PRINT '============================================================================';
PRINT 'FIX COMPLETE - ALL NVARCHAR COLUMNS NOW 500 CHARACTERS';
PRINT '============================================================================';
PRINT '';
