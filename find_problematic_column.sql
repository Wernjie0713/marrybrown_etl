-- Find which column is exactly 46 characters (causing the truncation error)

USE MarryBrown_DW;
GO

PRINT '=== ALL Column Sizes in staging_sales ===';
GO

SELECT 
    c.name AS ColumnName,
    t.name AS DataType,
    c.max_length AS MaxLength,
    CASE 
        WHEN t.name LIKE 'n%char' THEN c.max_length / 2 
        WHEN t.name LIKE '%char' THEN c.max_length
        ELSE NULL
    END AS CharLength,
    c.is_nullable AS IsNullable
FROM sys.columns c
INNER JOIN sys.types t ON c.user_type_id = t.user_type_id
WHERE c.object_id = OBJECT_ID('dbo.staging_sales')
ORDER BY c.column_id;
GO

PRINT '';
PRINT '=== Columns with CharLength around 46 ===';
GO

SELECT 
    c.name AS ColumnName,
    t.name AS DataType,
    CASE 
        WHEN t.name LIKE 'n%char' THEN c.max_length / 2 
        WHEN t.name LIKE '%char' THEN c.max_length
        ELSE c.max_length
    END AS CharLength
FROM sys.columns c
INNER JOIN sys.types t ON c.user_type_id = t.user_type_id
WHERE c.object_id = OBJECT_ID('dbo.staging_sales')
    AND (
        (t.name LIKE 'n%char' AND c.max_length / 2 BETWEEN 40 AND 50)
        OR (t.name LIKE '%char' AND c.max_length BETWEEN 40 AND 50)
    )
ORDER BY CharLength;
GO

