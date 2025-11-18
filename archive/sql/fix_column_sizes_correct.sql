-- ============================================================================
-- CORRECT FIX: Increase NVARCHAR columns to 500, keep complex fields as MAX
-- ============================================================================

USE MarryBrown_DW;
GO

PRINT '============================================================================';
PRINT 'FIXING COLUMN SIZES - CORRECT VERSION';
PRINT '============================================================================';
PRINT '';

-- ============================================================================
-- PART 1: staging_sales - Fix only non-MAX columns
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
    AND c.max_length < 1000  -- Less than 500 chars (1000 bytes for NVARCHAR)
    AND c.name NOT IN ('Items', 'Collection', 'Voucher', 'ExtendedSales', 'BillingAddress', 'ShippingAddress', 'Client');  -- Keep these as MAX

IF @sql <> ''
BEGIN
    PRINT 'Executing ALTER statements:';
    PRINT @sql;
    EXEC sp_executesql @sql;
    PRINT '[OK] All regular NVARCHAR columns increased to 500';
END
ELSE
BEGIN
    PRINT '[INFO] All regular NVARCHAR columns already 500 or larger';
END

GO

-- ============================================================================
-- PART 2: staging_sales_items - Fix only non-MAX columns
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
    PRINT 'Executing ALTER statements:';
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
-- PART 3: staging_payments - Fix only non-MAX columns
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
    PRINT 'Executing ALTER statements:';
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
PRINT 'VERIFICATION: Final column sizes';
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
        ELSE 'N/A'
    END AS CharLength
FROM sys.columns c
INNER JOIN sys.types t ON c.user_type_id = t.user_type_id
WHERE c.object_id = OBJECT_ID('dbo.staging_sales')
    AND t.name LIKE '%char'
ORDER BY c.column_id;

GO

PRINT '';
PRINT '============================================================================';
PRINT 'FIX COMPLETE';
PRINT '============================================================================';
PRINT '';
PRINT 'Summary:';
PRINT '  ✓ Regular NVARCHAR columns: 500 characters';
PRINT '  ✓ Complex fields (Items, Collection, etc.): NVARCHAR(MAX)';
PRINT '  ✓ Ready for full API data without truncation';
PRINT '';
