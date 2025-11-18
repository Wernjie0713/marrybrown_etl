-- Check for blocking sessions on staging tables
-- Run this if TRUNCATE is stuck

USE MarryBrown_DW;
GO

-- Show all active sessions on this database
SELECT 
    session_id,
    login_name,
    host_name,
    program_name,
    status,
    command,
    wait_type,
    wait_time,
    blocking_session_id,
    DATEDIFF(MINUTE, last_request_start_time, GETDATE()) AS minutes_running
FROM sys.dm_exec_sessions
WHERE database_id = DB_ID('MarryBrown_DW')
    AND session_id <> @@SPID  -- Exclude current session
ORDER BY minutes_running DESC;
GO

-- Show what's blocking
SELECT 
    blocking.session_id AS blocking_session,
    blocked.session_id AS blocked_session,
    blocking.login_name AS blocking_user,
    blocked.login_name AS blocked_user,
    blocking.wait_type AS blocking_wait,
    blocked.wait_type AS blocked_wait
FROM sys.dm_exec_sessions AS blocked
INNER JOIN sys.dm_exec_sessions AS blocking
    ON blocked.blocking_session_id = blocking.session_id
WHERE blocked.database_id = DB_ID('MarryBrown_DW');
GO

-- OPTION 1: Kill specific session (UNCOMMENT AND REPLACE session_id)
-- KILL 52;  -- Replace 52 with actual session_id from above query

-- OPTION 2: Kill all Python/ETL sessions (BE CAREFUL!)
DECLARE @kill_sql NVARCHAR(MAX) = '';

SELECT @kill_sql = @kill_sql + 'KILL ' + CAST(session_id AS VARCHAR(10)) + '; '
FROM sys.dm_exec_sessions
WHERE database_id = DB_ID('MarryBrown_DW')
    AND program_name LIKE '%python%'
    AND session_id <> @@SPID;

IF LEN(@kill_sql) > 0
BEGIN
    PRINT 'Killing sessions: ' + @kill_sql;
    -- EXEC sp_executesql @kill_sql;  -- UNCOMMENT to actually execute
END
ELSE
BEGIN
    PRINT 'No Python sessions found to kill.';
END
GO

-- Verify tables are accessible
SELECT 'staging_sales' AS TableName, COUNT(*) AS RowCount FROM staging_sales
UNION ALL
SELECT 'staging_sales_items', COUNT(*) FROM staging_sales_items
UNION ALL
SELECT 'staging_payments', COUNT(*) FROM staging_payments;
GO

