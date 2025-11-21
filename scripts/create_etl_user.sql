-- ============================================
-- Create ETL User for Python Scripts
-- Run this script in SQL Server Management Studio (SSMS)
-- ============================================

USE master;
GO

-- Step 1: Check if login exists
PRINT '======================================';
PRINT 'Step 1: Checking if login exists';
PRINT '======================================';
SELECT name, type_desc, create_date, is_disabled
FROM sys.server_principals
WHERE name = 'etl_user';
GO

-- Step 2: Drop and recreate login (if needed)
PRINT '';
PRINT '======================================';
PRINT 'Step 2: Creating/Updating login';
PRINT '======================================';

-- Drop login if exists
IF EXISTS (SELECT 1 FROM sys.server_principals WHERE name = 'etl_user')
BEGIN
    PRINT 'Dropping existing login...';
    DROP LOGIN etl_user;
END
GO

-- Create new login with SQL authentication
-- ⚠️ IMPORTANT: Change the password to match your .env.local file
PRINT 'Creating new login...';
CREATE LOGIN etl_user 
WITH PASSWORD = '8f9633a3',  -- ⚠️ This matches your .env.local - change if needed
     DEFAULT_DATABASE = MarryBrown_DW,
     CHECK_POLICY = OFF,      -- Disable password policy for testing
     CHECK_EXPIRATION = OFF;
GO

PRINT '[OK] Login created successfully';
GO

-- Step 3: Grant server-level permissions (optional but helpful)
PRINT '';
PRINT '======================================';
PRINT 'Step 3: Granting server permissions';
PRINT '======================================';
GRANT VIEW ANY DATABASE TO etl_user;
GO
PRINT '[OK] Server permissions granted';
GO

-- Step 4: Create database user and grant permissions
USE MarryBrown_DW;
GO

PRINT '';
PRINT '======================================';
PRINT 'Step 4: Setting up database user';
PRINT '======================================';

-- Drop user if exists
IF EXISTS (SELECT 1 FROM sys.database_principals WHERE name = 'etl_user')
BEGIN
    PRINT 'Dropping existing database user...';
    DROP USER etl_user;
END
GO

-- Create database user
PRINT 'Creating database user...';
CREATE USER etl_user FOR LOGIN etl_user;
GO
PRINT '[OK] Database user created';
GO

-- Add to db_owner role (full permissions for ETL operations)
PRINT 'Adding to db_owner role...';
ALTER ROLE db_owner ADD MEMBER etl_user;
GO
PRINT '[OK] Added to db_owner role';
GO

-- Step 5: Verify setup
PRINT '';
PRINT '======================================';
PRINT 'Step 5: Verification';
PRINT '======================================';

-- Check login
SELECT 'LOGIN CHECK:' as CheckType;
SELECT name, type_desc, is_disabled, create_date
FROM sys.server_principals
WHERE name = 'etl_user';
GO

-- Check database user
SELECT 'DATABASE USER CHECK:' as CheckType;
SELECT name, type_desc, create_date
FROM sys.database_principals
WHERE name = 'etl_user';
GO

-- Check roles
SELECT 'ROLE MEMBERSHIP:' as CheckType;
SELECT r.name as RoleName
FROM sys.database_role_members rm
JOIN sys.database_principals r ON rm.role_principal_id = r.principal_id
JOIN sys.database_principals u ON rm.member_principal_id = u.principal_id
WHERE u.name = 'etl_user';
GO

PRINT '';
PRINT '======================================';
PRINT '[SUCCESS] Setup complete!';
PRINT '======================================';
PRINT '';
PRINT 'Connection details for .env.local:';
PRINT '  TARGET_USERNAME=etl_user';
PRINT '  TARGET_PASSWORD=8f9633a3';
PRINT '';
PRINT 'Test the connection by running:';
PRINT '  python tests\test_local_connection.py';
PRINT '';
GO

