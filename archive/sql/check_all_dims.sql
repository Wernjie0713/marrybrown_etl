USE MarryBrown_DW;
GO

PRINT 'Checking all dimension tables...';
PRINT '';

-- dim_date
SELECT 'dim_date' as [Table], COUNT(*) as [Rows]
FROM dbo.dim_date;

-- dim_time
SELECT 'dim_time' as [Table], COUNT(*) as [Rows]
FROM dbo.dim_time;

-- dim_locations
SELECT 'dim_locations' as [Table], COUNT(*) as [Rows]
FROM dbo.dim_locations;

-- dim_products
SELECT 'dim_products' as [Table], COUNT(*) as [Rows]
FROM dbo.dim_products;

-- dim_staff
SELECT 'dim_staff' as [Table], COUNT(*) as [Rows]
FROM dbo.dim_staff;

-- dim_payment_types
SELECT 'dim_payment_types' as [Table], COUNT(*) as [Rows]
FROM dbo.dim_payment_types;

-- dim_promotions
SELECT 'dim_promotions' as [Table], COUNT(*) as [Rows]
FROM dbo.dim_promotions;

-- dim_terminals
SELECT 'dim_terminals' as [Table], COUNT(*) as [Rows]
FROM dbo.dim_terminals;

PRINT '';
PRINT 'Check complete!';

