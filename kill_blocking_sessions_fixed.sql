-- Check and kill blocking sessions - FIXED for SQL Server 2022
-- Run this if TRUNCATE is stuck

USE MarryBrown_DW;
GO

PRINT '=== Checking Active Sessions ===';
GO

-- Show all active sessions on this database (simplified)
SELECT 
    session_id,
    login_name,
    host_name,
    program_name,
    status,
    DATEDIFF(MINUTE, last_request_start_time, GETDATE()) AS minutes_running
FROM sys.dm_exec_sessions
WHERE database_id = DB_ID('MarryBrown_DW')
    AND session_id <> @@SPID  -- Exclude current session
ORDER BY minutes_running DESC;
GO

PRINT '=== Checking Table Locks ===';
GO

-- Show what tables are locked
SELECT 
    request_session_id AS session_id,
    resource_type,
    resource_database_id,
    DB_NAME(resource_database_id) AS database_name,
    OBJECT_NAME(resource_associated_entity_id, resource_database_id) AS table_name,
    request_mode,
    request_status
FROM sys.dm_tran_locks
WHERE resource_database_id = DB_ID('MarryBrown_DW')
    AND resource_type = 'OBJECT'
ORDER BY request_session_id;
GO

PRINT '=== Killing Python Sessions ===';
GO

-- Kill all Python sessions (be careful!)
DECLARE @kill_sql NVARCHAR(MAX) = '';
DECLARE @session_id INT;

DECLARE session_cursor CURSOR FOR
SELECT session_id
FROM sys.dm_exec_sessions
WHERE database_id = DB_ID('MarryBrown_DW')
    AND program_name LIKE '%python%'
    AND session_id <> @@SPID;

OPEN session_cursor;
FETCH NEXT FROM session_cursor INTO @session_id;

WHILE @@FETCH_STATUS = 0
BEGIN
    SET @kill_sql = 'KILL ' + CAST(@session_id AS VARCHAR(10));
    PRINT 'Executing: ' + @kill_sql;
    
    BEGIN TRY
        EXEC sp_executesql @kill_sql;
        PRINT '  SUCCESS: Session ' + CAST(@session_id AS VARCHAR(10)) + ' killed';
    END TRY
    BEGIN CATCH
        PRINT '  ERROR: Could not kill session ' + CAST(@session_id AS VARCHAR(10));
    END CATCH
    
    FETCH NEXT FROM session_cursor INTO @session_id;
END

CLOSE session_cursor;
DEALLOCATE session_cursor;
GO

PRINT '=== Verifying Tables Are Accessible ===';
GO

-- Verify tables are accessible
SELECT 'staging_sales' AS TableName, COUNT(*) AS RowCount FROM staging_sales;
SELECT 'staging_sales_items' AS TableName, COUNT(*) AS RowCount FROM staging_sales_items;
SELECT 'staging_payments' AS TableName, COUNT(*) AS RowCount FROM staging_payments;
GO

PRINT '=== DONE ===';
GO

