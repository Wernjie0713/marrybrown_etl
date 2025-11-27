-- Extend etl_replica_progress table with checkpoint fields for resume functionality
-- Run this after 110_create_replica_metadata_tables.sql

USE MarryBrown_DW;
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.columns 
    WHERE object_id = OBJECT_ID('dbo.etl_replica_progress') 
    AND name = 'last_chunk_id'
)
BEGIN
    ALTER TABLE dbo.etl_replica_progress
    ADD last_chunk_id BIGINT NULL,
        rows_processed BIGINT NULL,
        checkpoint_data NVARCHAR(MAX) NULL;
    
    PRINT 'Added checkpoint fields to etl_replica_progress table.';
END
ELSE
BEGIN
    PRINT 'Checkpoint fields already exist in etl_replica_progress table.';
END
GO

