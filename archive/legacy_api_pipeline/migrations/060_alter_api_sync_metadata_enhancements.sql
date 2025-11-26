/*
Enhance dbo.api_sync_metadata for adaptive resume + metrics.
*/

USE MarryBrown_DW;
GO

PRINT 'Updating dbo.api_sync_metadata...';
GO

IF COL_LENGTH('dbo.api_sync_metadata', 'JobName') IS NULL
BEGIN
    PRINT '  - Adding JobName column';
    ALTER TABLE dbo.api_sync_metadata
        ADD JobName NVARCHAR(100) NULL;
END
ELSE
BEGIN
    PRINT '  - JobName column already present';
END
GO

-- Update existing rows with JobName based on SyncID
IF COL_LENGTH('dbo.api_sync_metadata', 'JobName') IS NOT NULL
BEGIN
    UPDATE dbo.api_sync_metadata
        SET JobName = CONCAT('legacy_', SyncID)
        WHERE JobName IS NULL;
    
    -- Make JobName NOT NULL after populating existing rows
    ALTER TABLE dbo.api_sync_metadata
        ALTER COLUMN JobName NVARCHAR(100) NOT NULL;

    -- Create unique index if it doesn't exist
    IF NOT EXISTS (
        SELECT 1 
        FROM sys.indexes 
        WHERE name = 'UX_api_sync_metadata_job'
          AND object_id = OBJECT_ID('dbo.api_sync_metadata')
    )
    BEGIN
        CREATE UNIQUE INDEX UX_api_sync_metadata_job
            ON dbo.api_sync_metadata(JobName);
        PRINT '  - Created unique index on JobName';
    END
END
GO

IF COL_LENGTH('dbo.api_sync_metadata', 'LastChunkNumber') IS NULL
BEGIN
    PRINT '  - Adding chunk metric columns';
    ALTER TABLE dbo.api_sync_metadata
        ADD LastChunkNumber INT NULL,
            LastChunkRowCount INT NULL,
            LastChunkDurationSeconds DECIMAL(10,2) NULL,
            LastChunkCompletedAt DATETIME NULL;
END
ELSE
BEGIN
    PRINT '  - Chunk metric columns already present';
END
GO

PRINT 'dbo.api_sync_metadata upgrade complete.';
GO


