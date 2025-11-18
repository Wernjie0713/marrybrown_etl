-- ============================================================================
-- CHECK ACTUAL SCHEMA IN DATABASE
-- ============================================================================

USE MarryBrown_DW;
GO

PRINT '============================================================================';
PRINT 'ACTUAL SCHEMA IN dbo.staging_sales';
PRINT '============================================================================';
PRINT '';

SELECT 
    COLUMN_ID = c.column_id,
    COLUMN_NAME = c.name,
    DATA_TYPE = t.name,
    MAX_LENGTH = c.max_length,
    IS_NULLABLE = c.is_nullable,
    CHAR_LENGTH = CASE 
        WHEN t.name LIKE 'n%char' THEN 
            CASE 
                WHEN c.max_length = -1 THEN 'MAX'
                ELSE CAST(c.max_length / 2 AS VARCHAR(10))
            END
        WHEN t.name LIKE '%char' THEN CAST(c.max_length AS VARCHAR(10))
        ELSE 'N/A'
    END
FROM sys.columns c
INNER JOIN sys.types t ON c.user_type_id = t.user_type_id
WHERE c.object_id = OBJECT_ID('dbo.staging_sales')
ORDER BY c.column_id;

GO

PRINT '';
PRINT '============================================================================';
PRINT 'COLUMNS WITH SIZE 46 OR LESS (THE PROBLEM):';
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
        (t.name LIKE 'n%char' AND c.max_length / 2 <= 46)
        OR (t.name LIKE '%char' AND c.max_length <= 46)
    )
ORDER BY c.column_id;

GO
