/*
FIX: dim_locations Schema Mismatch
Changes LocationID to LocationGUID to match ETL script expectations

Run this if you already deployed the schema and got "Invalid column name 'LocationGUID'" error
*/

USE MarryBrown_DW;
GO

PRINT '========================================';
PRINT 'FIXING dim_locations SCHEMA';
PRINT '========================================';
PRINT '';

-- Check if table exists
IF EXISTS (SELECT * FROM sys.tables WHERE name = 'dim_locations')
BEGIN
    -- Check if it has the wrong column (LocationID) instead of LocationGUID
    IF EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('dbo.dim_locations') AND name = 'LocationID')
    BEGIN
        PRINT 'Found LocationID column - fixing...';
        
        -- Drop the table and recreate with correct schema
        DROP TABLE dbo.dim_locations;
        PRINT '  [OK] Dropped old table';
        
        -- Recreate with correct schema
        CREATE TABLE dbo.dim_locations (
            LocationKey INT IDENTITY(1,1) PRIMARY KEY,
            LocationGUID NVARCHAR(50) NOT NULL,
            LocationName NVARCHAR(200),
            City NVARCHAR(100),
            State NVARCHAR(100),
            IsActive BIT DEFAULT 1
        );
        PRINT '  [OK] Created new table with LocationGUID column';
    END
    ELSE IF EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('dbo.dim_locations') AND name = 'LocationGUID')
    BEGIN
        PRINT '  - Table already has correct schema (LocationGUID exists)';
    END
    ELSE
    BEGIN
        PRINT '  [WARNING] Table exists but has unexpected schema';
    END
END
ELSE
BEGIN
    -- Table doesn't exist yet, create it with correct schema
    CREATE TABLE dbo.dim_locations (
        LocationKey INT IDENTITY(1,1) PRIMARY KEY,
        LocationGUID NVARCHAR(50) NOT NULL,
        LocationName NVARCHAR(200),
        City NVARCHAR(100),
        State NVARCHAR(100),
        IsActive BIT DEFAULT 1
    );
    PRINT '  [OK] Created dim_locations with LocationGUID column';
END

PRINT '';
PRINT '========================================';
PRINT 'FIX COMPLETE!';
PRINT '========================================';
PRINT '';
PRINT 'Now you can run: python etl_dim_locations.py';
PRINT '';

