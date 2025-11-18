/*
ETL Progress Tracking Table
Author: YONG WERN JIE
Date: November 7, 2025

Purpose: Track ETL extraction progress to enable true resume capability
*/

-- Create etl_progress table if not exists
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'etl_progress')
BEGIN
    CREATE TABLE dbo.etl_progress (
        job_id VARCHAR(100) PRIMARY KEY,
        last_timestamp BIGINT NULL,
        last_call_count INT NULL,
        start_date DATE NULL,
        end_date DATE NULL,
        chunk_size INT NULL,
        last_updated DATETIME2 DEFAULT GETDATE(),
        total_sales_loaded INT DEFAULT 0,
        total_items_loaded INT DEFAULT 0,
        total_payments_loaded INT DEFAULT 0,
        status VARCHAR(50) DEFAULT 'IN_PROGRESS',
        error_message NVARCHAR(MAX) NULL
    );
    
    PRINT 'Created table: etl_progress';
END
ELSE
BEGIN
    PRINT 'Table etl_progress already exists';
END
GO

-- Create index on last_updated for performance
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_etl_progress_last_updated')
BEGIN
    CREATE INDEX IX_etl_progress_last_updated ON dbo.etl_progress(last_updated);
    PRINT 'Created index: IX_etl_progress_last_updated';
END
GO

-- Insert default job if not exists
IF NOT EXISTS (SELECT * FROM dbo.etl_progress WHERE job_id = 'sales_extraction')
BEGIN
    INSERT INTO dbo.etl_progress (job_id, status) 
    VALUES ('sales_extraction', 'READY');
    PRINT 'Initialized default job: sales_extraction';
END
GO

PRINT 'ETL Progress table setup complete!';
GO

