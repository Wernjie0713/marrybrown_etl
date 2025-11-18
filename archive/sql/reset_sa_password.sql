-- Reset SA password
-- Run this in SSMS on the cloud server using Windows Authentication

USE master;
GO

-- Reset sa password to a new one
ALTER LOGIN sa WITH PASSWORD = 'NewSA@Password2025!';
GO

-- Make sure sa is enabled
ALTER LOGIN sa ENABLE;
GO

PRINT '[OK] SA password has been reset';
PRINT 'New password: NewSA@Password2025!';
GO

-- Test the login
SELECT 
    name,
    type_desc,
    is_disabled,
    create_date,
    modify_date
FROM sys.server_principals
WHERE name = 'sa';
GO

