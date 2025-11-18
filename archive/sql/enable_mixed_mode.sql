-- Enable Mixed Mode Authentication (SQL Server + Windows)
-- Run this in SSMS on the cloud server using Windows Authentication

-- Check current authentication mode
EXEC xp_instance_regread 
    N'HKEY_LOCAL_MACHINE', 
    N'Software\Microsoft\MSSQLServer\MSSQLServer',
    N'LoginMode';
-- 1 = Windows Authentication Only
-- 2 = Mixed Mode (SQL Server and Windows Authentication)

PRINT '';
PRINT 'Current authentication mode shown above (1=Windows Only, 2=Mixed Mode)';
PRINT '';
PRINT 'Enabling Mixed Mode authentication...';
PRINT '';

-- Enable Mixed Mode (allows SQL Server logins)
EXEC xp_instance_regwrite 
    N'HKEY_LOCAL_MACHINE', 
    N'Software\Microsoft\MSSQLServer\MSSQLServer',
    N'LoginMode', 
    REG_DWORD, 
    2;  -- 2 = Mixed Mode

PRINT '[OK] Mixed Mode authentication enabled!';
PRINT '';
PRINT '⚠️  IMPORTANT: You MUST restart SQL Server service for this to take effect!';
PRINT '';
PRINT 'To restart SQL Server:';
PRINT '  1. Press Win+R → services.msc → Enter';
PRINT '  2. Find "SQL Server (MSSQLSERVER)"';
PRINT '  3. Right-click → Restart';
PRINT '';
PRINT 'OR run this in PowerShell as Administrator:';
PRINT '  Restart-Service MSSQLSERVER';
PRINT '';
GO

