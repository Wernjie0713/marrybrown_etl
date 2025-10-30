-- Enable Mixed Mode Authentication (SQL Server + Windows)
-- Run this in SSMS on the cloud server using Windows Authentication

USE master;
GO

PRINT '======================================';
PRINT 'Step 1: Check Current Authentication Mode';
PRINT '======================================';

-- Check current mode
DECLARE @LoginMode INT;
EXEC xp_instance_regread 
    N'HKEY_LOCAL_MACHINE', 
    N'Software\Microsoft\MSSQLServer\MSSQLServer',
    N'LoginMode',
    @LoginMode OUTPUT;

IF @LoginMode = 1
    PRINT 'Current Mode: Windows Authentication Only'
ELSE IF @LoginMode = 2
    PRINT 'Current Mode: Mixed Mode (SQL Server and Windows Authentication)'
ELSE
    PRINT 'Current Mode: Unknown';

PRINT '';
PRINT '======================================';
PRINT 'Step 2: Enable Mixed Mode';
PRINT '======================================';

-- Enable Mixed Mode (allows SQL Server logins)
BEGIN TRY
    EXEC xp_instance_regwrite 
        N'HKEY_LOCAL_MACHINE', 
        N'Software\Microsoft\MSSQLServer\MSSQLServer',
        N'LoginMode', 
        REG_DWORD, 
        2;  -- 2 = Mixed Mode
    
    PRINT '[OK] Registry updated successfully';
END TRY
BEGIN CATCH
    PRINT '[ERROR] Failed to update registry:';
    PRINT ERROR_MESSAGE();
    PRINT '';
    PRINT 'You may need to run SSMS as Administrator';
END CATCH

PRINT '';
PRINT '======================================';
PRINT 'Step 3: Verify Change';
PRINT '======================================';

-- Verify the change
EXEC xp_instance_regread 
    N'HKEY_LOCAL_MACHINE', 
    N'Software\Microsoft\MSSQLServer\MSSQLServer',
    N'LoginMode';

PRINT '';
PRINT 'If LoginMode = 2, the change was successful!';
PRINT '';

PRINT '======================================';
PRINT 'Step 4: RESTART SQL SERVER (REQUIRED)';
PRINT '======================================';
PRINT '';
PRINT 'The change will NOT take effect until you restart!';
PRINT '';
PRINT 'Option 1: PowerShell (as Administrator):';
PRINT '  Restart-Service MSSQLSERVER';
PRINT '';
PRINT 'Option 2: Services GUI:';
PRINT '  1. Press Win+R → services.msc → Enter';
PRINT '  2. Find "SQL Server (MSSQLSERVER)"';
PRINT '  3. Right-click → Restart';
PRINT '';
PRINT 'Option 3: Command Prompt (as Administrator):';
PRINT '  net stop MSSQLSERVER';
PRINT '  net start MSSQLSERVER';
PRINT '';
GO

