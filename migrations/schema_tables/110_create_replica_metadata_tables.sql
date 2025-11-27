PRINT 'Creating replica metadata tables';

IF OBJECT_ID('dbo.etl_replica_progress', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.etl_replica_progress (
        id INT IDENTITY(1,1) PRIMARY KEY,
        table_name NVARCHAR(200) NOT NULL,
        job_date DATE NOT NULL,
        batch_start DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        batch_end DATETIME2 NULL,
        rows_extracted BIGINT NULL,
        rows_loaded BIGINT NULL,
        status NVARCHAR(50) NOT NULL,
        message NVARCHAR(MAX) NULL
    );

    CREATE INDEX IX_etl_replica_progress_table_date
        ON dbo.etl_replica_progress (table_name, job_date);
END
GO

IF OBJECT_ID('dbo.replica_run_history', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.replica_run_history (
        id INT IDENTITY(1,1) PRIMARY KEY,
        run_id UNIQUEIDENTIFIER NOT NULL DEFAULT NEWID(),
        run_type NVARCHAR(20) NOT NULL, -- T0, T1, backfill, etc.
        start_timestamp DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        end_timestamp DATETIME2 NULL,
        start_date DATE NULL,
        end_date DATE NULL,
        processed_tables NVARCHAR(MAX) NULL,
        success BIT NOT NULL DEFAULT 0,
        error_message NVARCHAR(MAX) NULL
    );
END
GO

PRINT 'Replica metadata tables ready.';


