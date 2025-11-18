-- ============================================================================
-- FIX: Change complex fields back to NVARCHAR(MAX)
-- ============================================================================

USE MarryBrown_DW;
GO

PRINT '============================================================================';
PRINT 'FIXING COMPLEX FIELDS TO NVARCHAR(MAX)';
PRINT '============================================================================';
PRINT '';

-- ============================================================================
-- staging_sales: Change complex fields to MAX
-- ============================================================================
PRINT 'PART 1: Fixing staging_sales complex fields...';
PRINT '';

ALTER TABLE dbo.staging_sales ALTER COLUMN Items NVARCHAR(MAX);
PRINT '  [OK] Items: NVARCHAR(500) -> NVARCHAR(MAX)';

ALTER TABLE dbo.staging_sales ALTER COLUMN Collection NVARCHAR(MAX);
PRINT '  [OK] Collection: NVARCHAR(500) -> NVARCHAR(MAX)';

ALTER TABLE dbo.staging_sales ALTER COLUMN Voucher NVARCHAR(MAX);
PRINT '  [OK] Voucher: NVARCHAR(500) -> NVARCHAR(MAX)';

ALTER TABLE dbo.staging_sales ALTER COLUMN ExtendedSales NVARCHAR(MAX);
PRINT '  [OK] ExtendedSales: NVARCHAR(500) -> NVARCHAR(MAX)';

ALTER TABLE dbo.staging_sales ALTER COLUMN BillingAddress NVARCHAR(MAX);
PRINT '  [OK] BillingAddress: NVARCHAR(500) -> NVARCHAR(MAX)';

ALTER TABLE dbo.staging_sales ALTER COLUMN ShippingAddress NVARCHAR(MAX);
PRINT '  [OK] ShippingAddress: NVARCHAR(500) -> NVARCHAR(MAX)';

ALTER TABLE dbo.staging_sales ALTER COLUMN Client NVARCHAR(MAX);
PRINT '  [OK] Client: NVARCHAR(500) -> NVARCHAR(MAX)';

GO

PRINT '';
PRINT '============================================================================';
PRINT 'VERIFICATION: Final column sizes';
PRINT '============================================================================';
PRINT '';

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
PRINT 'FIX COMPLETE - COMPLEX FIELDS NOW NVARCHAR(MAX)';
PRINT '============================================================================';
PRINT '';
PRINT 'Summary:';
PRINT '  ✓ Regular NVARCHAR columns: 500 characters';
PRINT '  ✓ Complex fields (Items, Collection, etc.): NVARCHAR(MAX)';
PRINT '  ✓ Ready for full API data without truncation';
PRINT '';
