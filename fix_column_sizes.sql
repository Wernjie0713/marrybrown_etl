/*
=============================================================================
FIX COLUMN SIZES FOR dim_customers AND dim_promotions
Run this to fix truncation errors without dropping tables
=============================================================================
*/

USE MarryBrown_DW;
GO

PRINT 'Fixing column sizes...';
PRINT '';

-- Fix dim_customers.CustomerCode (50 -> 500)
IF EXISTS (SELECT * FROM sys.tables WHERE name = 'dim_customers')
BEGIN
    PRINT 'Altering dim_customers.CustomerCode to NVARCHAR(500)...';
    ALTER TABLE dbo.dim_customers
    ALTER COLUMN CustomerCode NVARCHAR(500);
    PRINT '  [OK] CustomerCode resized to 500 characters';
END

-- Fix dim_promotions.PromotionCode (50 -> 200)
IF EXISTS (SELECT * FROM sys.tables WHERE name = 'dim_promotions')
BEGIN
    PRINT 'Altering dim_promotions.PromotionCode to NVARCHAR(200)...';
    ALTER TABLE dbo.dim_promotions
    ALTER COLUMN PromotionCode NVARCHAR(200);
    PRINT '  [OK] PromotionCode resized to 200 characters';
END

PRINT '';
PRINT '========================================';
PRINT 'COLUMN SIZE FIXES COMPLETE!';
PRINT '========================================';
PRINT '';
PRINT 'You can now re-run:';
PRINT '  python etl_dim_customers.py';
PRINT '  python etl_dim_promotions.py';
PRINT '';

