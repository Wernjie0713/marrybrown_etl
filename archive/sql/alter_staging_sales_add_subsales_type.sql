-- Add SUBSALES_TYPE column to staging_sales table
IF NOT EXISTS (SELECT * FROM sys.columns 
               WHERE Name = N'SUBSALES_TYPE' AND Object_ID = Object_ID(N'dbo.staging_sales'))
BEGIN
    ALTER TABLE dbo.staging_sales
    ADD SUBSALES_TYPE VARCHAR(100);
    PRINT 'Column SUBSALES_TYPE added to dbo.staging_sales table.';
END
ELSE
BEGIN
    PRINT 'Column SUBSALES_TYPE already exists in dbo.staging_sales table.';
END
GO

