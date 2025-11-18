-- Add double_total_tax_amount column to staging_sales_items table
-- This field captures the total tax amount (item-level + bill-level tax allocation)
-- which Xilnex uses for "Sales Amount ex. MGST Tax" calculations

IF NOT EXISTS (SELECT * FROM sys.columns 
               WHERE Name = N'double_total_tax_amount' AND Object_ID = Object_ID(N'dbo.staging_sales_items'))
BEGIN
    ALTER TABLE dbo.staging_sales_items
    ADD double_total_tax_amount DECIMAL(18,4);
    PRINT 'Column double_total_tax_amount added to dbo.staging_sales_items table.';
END
ELSE
BEGIN
    PRINT 'Column double_total_tax_amount already exists in dbo.staging_sales_items table.';
END
GO

