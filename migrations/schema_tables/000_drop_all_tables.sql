-- Drop all replica tables and foreign key constraints
-- WARNING: This will delete all data in replica tables!

USE MarryBrown_DW;
GO

PRINT 'Dropping all foreign key constraints...';
GO

-- Drop all foreign keys
DECLARE @sql NVARCHAR(MAX) = N'';

SELECT @sql = @sql + N'
ALTER TABLE ' + QUOTENAME(s.name) + '.' + QUOTENAME(t.name) +
' DROP CONSTRAINT ' + QUOTENAME(fk.name) + ';'
FROM sys.foreign_keys fk
JOIN sys.tables t ON fk.parent_object_id = t.object_id
JOIN sys.schemas s ON t.schema_id = s.schema_id
WHERE t.name LIKE 'com_5013_%';

IF @sql <> N''
BEGIN
    EXEC sys.sp_executesql @sql;
    PRINT 'Foreign key constraints dropped.';
END
ELSE
BEGIN
    PRINT 'No foreign key constraints found.';
END
GO

PRINT 'Dropping all replica tables...';
GO

-- Drop all replica tables
DECLARE @sql2 NVARCHAR(MAX) = N'';

SELECT @sql2 = @sql2 + N'
DROP TABLE ' + QUOTENAME(s.name) + '.' + QUOTENAME(t.name) + ';'
FROM sys.tables t
JOIN sys.schemas s ON t.schema_id = s.schema_id
WHERE t.name LIKE 'com_5013_%';

IF @sql2 <> N''
BEGIN
    EXEC sys.sp_executesql @sql2;
    PRINT 'Replica tables dropped.';
END
ELSE
BEGIN
    PRINT 'No replica tables found.';
END
GO

PRINT 'Done.';
GO

