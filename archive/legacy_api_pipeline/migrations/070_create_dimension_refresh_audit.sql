/*
Create audit table used by dimension CDC helpers.
*/

USE MarryBrown_DW;
GO

IF OBJECT_ID('dbo.dimension_refresh_audit', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.dimension_refresh_audit (
        DimensionName NVARCHAR(100) NOT NULL PRIMARY KEY,
        SourceHash CHAR(64) NOT NULL,
        [RowCount] INT NOT NULL,
        LastRunUTC DATETIME NOT NULL DEFAULT SYSUTCDATETIME(),
        LastDurationSeconds DECIMAL(10,2) NULL
    );
    PRINT '  [OK] Created dbo.dimension_refresh_audit';
END
ELSE
BEGIN
    PRINT '  - dbo.dimension_refresh_audit already exists';
END
GO


