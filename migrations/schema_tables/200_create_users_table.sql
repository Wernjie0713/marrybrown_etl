PRINT 'Creating dbo.api_users table';
GO

IF OBJECT_ID('dbo.api_users', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.api_users (
        id INT IDENTITY(1,1) PRIMARY KEY,
        email NVARCHAR(255) NOT NULL UNIQUE,
        hashed_password NVARCHAR(255) NOT NULL,
        is_active BIT NOT NULL DEFAULT (1),
        is_superuser BIT NOT NULL DEFAULT (0),
        created_at DATETIME NOT NULL DEFAULT (GETDATE()),
        updated_at DATETIME NOT NULL DEFAULT (GETDATE())
    );
    PRINT 'Table dbo.api_users created.';
END
ELSE
BEGIN
    PRINT 'Table dbo.api_users already exists.';
END;
GO

-- Insert sample user if not already present
IF NOT EXISTS (SELECT 1 FROM dbo.api_users WHERE email = 'user@example.com')
BEGIN
    INSERT INTO dbo.api_users (email, hashed_password, is_active, is_superuser)
    VALUES (
        'user@example.com',
        '$2b$12$KIX0l3t1PZqO6Qm3r.j7ge4Fh6gl.iEzq8X5p1f/vKOcVnXV/1L.G', -- bcrypt hash for "password"
        1,
        0
    );
    PRINT 'Inserted sample user user@example.com';
END
ELSE
BEGIN
    PRINT 'Sample user user@example.com already exists.';
END;
GO
