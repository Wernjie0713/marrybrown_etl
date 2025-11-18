/*
Fix last_timestamp column type from BIGINT to VARCHAR
Author: YONG WERN JIE  
Date: November 7, 2025

Purpose: Fix data type mismatch error when saving progress
Issue: last_timestamp was BIGINT but receiving hex string like '0x000000009588D0BA'
Solution: Change to VARCHAR(50) to store hex timestamp strings
*/

-- Alter the column type to VARCHAR to store hex timestamp strings
IF EXISTS (SELECT * FROM sys.tables WHERE name = 'etl_progress')
BEGIN
    -- Clear any existing data in the column first
    UPDATE dbo.etl_progress SET last_timestamp = NULL;
    
    -- Alter column type from BIGINT to VARCHAR(50)
    ALTER TABLE dbo.etl_progress 
    ALTER COLUMN last_timestamp VARCHAR(50) NULL;
    
    PRINT 'SUCCESS: Fixed last_timestamp column type: BIGINT -> VARCHAR(50)';
END
ELSE
BEGIN
    PRINT 'ERROR: Table etl_progress does not exist. Run 001_create_etl_progress_table.sql first.';
END
GO

PRINT 'Migration 002 complete!';
GO

