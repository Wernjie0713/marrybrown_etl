/*
Create metadata helpers for API sync progress tracking
Author: Repo Cleanup Automation
Date: November 18, 2025
*/

USE MarryBrown_DW;
GO

PRINT 'Creating metadata tables...';
GO

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'api_sync_metadata')
BEGIN
    CREATE TABLE dbo.api_sync_metadata (
        SyncID INT IDENTITY(1,1) PRIMARY KEY,
        LastTimestamp NVARCHAR(50),
        SyncStartTime DATETIME,
        SyncEndTime DATETIME,
        RecordsExtracted INT,
        Status NVARCHAR(20),
        ErrorMessage NVARCHAR(MAX),
        DateRangeStart DATE,
        DateRangeEnd DATE
    );
    PRINT '  [OK] Created api_sync_metadata';
END
ELSE PRINT '  - api_sync_metadata already exists';

PRINT 'Metadata table creation complete.';
GO

