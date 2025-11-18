-- ============================================================================
-- DIAGNOSE: Find the column that is exactly 46 characters
-- ============================================================================
-- Error: "String data, right truncation: length 48 buffer 46"
-- This means: trying to insert 48 chars into a 46-char column
-- ============================================================================

USE MarryBrown_DW;
GO

PRINT '============================================================================';
PRINT 'FINDING PROBLEMATIC COLUMN (46 characters)';
PRINT '============================================================================';
PRINT '';

PRINT 'All columns in staging_sales with their sizes:';
PRINT '';

SELECT 
    c.name AS ColumnName,
    t.name AS DataType,
    c.max_length AS MaxLength,
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
ORDER BY c.column_id;

GO

PRINT '';
PRINT '============================================================================';
PRINT 'COLUMNS WITH SIZE 46 (THE PROBLEM):';
PRINT '============================================================================';
PRINT '';

SELECT 
    c.name AS ColumnName,
    t.name AS DataType,
    CASE 
        WHEN t.name LIKE 'n%char' THEN c.max_length / 2 
        ELSE c.max_length 
    END AS CharLength
FROM sys.columns c
INNER JOIN sys.types t ON c.user_type_id = t.user_type_id
WHERE c.object_id = OBJECT_ID('dbo.staging_sales')
    AND (
        (t.name LIKE 'n%char' AND c.max_length / 2 = 46)
        OR (t.name LIKE '%char' AND c.max_length = 46)
    )
ORDER BY c.column_id;

GO

PRINT '';
PRINT '============================================================================';
PRINT 'FIX: Increase all remaining columns to 500 characters';
PRINT '============================================================================';
PRINT '';

-- Find all NVARCHAR columns that are NOT already 500 or MAX
SELECT 
    'ALTER TABLE dbo.staging_sales ALTER COLUMN ' + c.name + ' NVARCHAR(500);' AS SQLCommand,
    c.name AS ColumnName,
    CASE 
        WHEN t.name LIKE 'n%char' THEN c.max_length / 2 
        ELSE c.max_length 
    END AS CurrentSize
FROM sys.columns c
INNER JOIN sys.types t ON c.user_type_id = t.user_type_id
WHERE c.object_id = OBJECT_ID('dbo.staging_sales')
    AND t.name LIKE 'n%char'
    AND c.max_length < 1000  -- Less than 500 chars (1000 bytes for NVARCHAR)
ORDER BY c.column_id;

GO
