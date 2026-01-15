-- Drop all user tables in source_data database
-- Run this as SQL admin on the source_data database

USE [source_data];
GO

DECLARE @sql NVARCHAR(MAX) = N'';

-- Drop foreign key constraints first
SELECT @sql += N'ALTER TABLE [' + s.name + N'].[' + t.name + N'] DROP CONSTRAINT [' + f.name + N'];'
    + CHAR(13)
FROM sys.foreign_keys f
JOIN sys.tables t ON f.parent_object_id = t.object_id
JOIN sys.schemas s ON t.schema_id = s.schema_id;

EXEC sp_executesql @sql;

-- Drop all user tables
SET @sql = N'';
SELECT @sql += N'DROP TABLE [' + s.name + N'].[' + t.name + N'];'
    + CHAR(13)
FROM sys.tables t
JOIN sys.schemas s ON t.schema_id = s.schema_id
WHERE t.is_ms_shipped = 0;

EXEC sp_executesql @sql;

-- Verify no user tables remain
SELECT s.name AS SchemaName, t.name AS TableName
FROM sys.tables t
JOIN sys.schemas s ON t.schema_id = s.schema_id
WHERE t.is_ms_shipped = 0;

