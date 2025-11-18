-- Add SubSalesType column to fact_sales_transactions table
IF NOT EXISTS (SELECT * FROM sys.columns 
               WHERE Name = N'SubSalesType' AND Object_ID = Object_ID(N'dbo.fact_sales_transactions'))
BEGIN
    ALTER TABLE dbo.fact_sales_transactions
    ADD SubSalesType VARCHAR(100);
    PRINT 'Column SubSalesType added to dbo.fact_sales_transactions table.';
END
ELSE
BEGIN
    PRINT 'Column SubSalesType already exists in dbo.fact_sales_transactions table.';
END
GO

