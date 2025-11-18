-- Migration Script: Add double_amount column to staging_payments
-- Version: 1.7.0
-- Date: 2025-10-16
-- Purpose: Support split-tender payment allocation by storing payment amounts

-- Check if column already exists
IF NOT EXISTS (
    SELECT 1 
    FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = 'dbo' 
      AND TABLE_NAME = 'staging_payments' 
      AND COLUMN_NAME = 'double_amount'
)
BEGIN
    PRINT 'Adding double_amount column to staging_payments...';
    
    ALTER TABLE dbo.staging_payments
    ADD double_amount DECIMAL(18, 4);
    
    PRINT '✅ Column added successfully!';
END
ELSE
BEGIN
    PRINT 'ℹ️ Column double_amount already exists in staging_payments.';
END
GO

