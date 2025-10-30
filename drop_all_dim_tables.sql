/*
DROP ALL DIMENSION TABLES
Use this to clear wrong schema and let ETL scripts create the correct ones

Author: YONG WERN JIE
Date: October 29, 2025
*/

USE MarryBrown_DW;
GO

PRINT '========================================';
PRINT 'DROPPING ALL DIMENSION TABLES';
PRINT '========================================';
PRINT '';
PRINT 'This will allow ETL scripts to auto-create tables with correct schema';
PRINT '';

-- Drop in reverse order of dependencies
IF EXISTS (SELECT * FROM sys.tables WHERE name = 'dim_terminals')
BEGIN
    DROP TABLE dbo.dim_terminals;
    PRINT '  [OK] Dropped dim_terminals';
END

IF EXISTS (SELECT * FROM sys.tables WHERE name = 'dim_promotions')
BEGIN
    DROP TABLE dbo.dim_promotions;
    PRINT '  [OK] Dropped dim_promotions';
END

IF EXISTS (SELECT * FROM sys.tables WHERE name = 'dim_payment_types')
BEGIN
    DROP TABLE dbo.dim_payment_types;
    PRINT '  [OK] Dropped dim_payment_types';
END

IF EXISTS (SELECT * FROM sys.tables WHERE name = 'dim_staff')
BEGIN
    DROP TABLE dbo.dim_staff;
    PRINT '  [OK] Dropped dim_staff';
END

IF EXISTS (SELECT * FROM sys.tables WHERE name = 'dim_customers')
BEGIN
    DROP TABLE dbo.dim_customers;
    PRINT '  [OK] Dropped dim_customers';
END

IF EXISTS (SELECT * FROM sys.tables WHERE name = 'dim_products')
BEGIN
    DROP TABLE dbo.dim_products;
    PRINT '  [OK] Dropped dim_products';
END

IF EXISTS (SELECT * FROM sys.tables WHERE name = 'dim_locations')
BEGIN
    DROP TABLE dbo.dim_locations;
    PRINT '  [OK] Dropped dim_locations';
END

PRINT '';
PRINT '========================================';
PRINT 'ALL DIMENSION TABLES DROPPED!';
PRINT '========================================';
PRINT '';
PRINT 'Next steps:';
PRINT '  1. Run: python generate_time_dims.py  (creates dim_date & dim_time)';
PRINT '  2. Run: python etl_dim_locations.py   (auto-creates table + loads data)';
PRINT '  3. Run: python etl_dim_products.py    (auto-creates table + loads data)';
PRINT '  4. Run: python etl_dim_staff.py       (auto-creates table + loads data)';
PRINT '  5. Run: python etl_dim_payment_types.py';
PRINT '  6. Run: python etl_dim_customers.py';
PRINT '  7. Run: python etl_dim_promotions.py';
PRINT '  8. Run: python etl_dim_terminals.py';
PRINT '';
PRINT 'Pandas .to_sql() will create tables automatically with correct schema!';
PRINT '';

