-- SQL Script to grant Service Principal access to Azure SQL Database
-- Run this script as the Entra ID admin (bksdrodrigo@outlook.com)
-- Connect to the database: dataplatform

-- Service Principal Details:
-- App ID: 8eb12cc4-0cd6-4f3f-b862-72f24b62478d
-- Display Name: sp-dataplatform-sql
-- Object ID: be99dc09-21a7-486f-97cb-53d61cd2a7f1

USE [dataplatform];
GO

-- Create a contained database user for the service principal
-- Using the App ID (Client ID) of the service principal
IF NOT EXISTS (SELECT * FROM sys.database_principals WHERE name = 'sp-dataplatform-sql')
BEGIN
    CREATE USER [sp-dataplatform-sql] FROM EXTERNAL PROVIDER;
    PRINT 'Service principal user created successfully';
END
ELSE
BEGIN
    PRINT 'Service principal user already exists';
END
GO

-- Grant database roles based on your requirements
-- Option 1: Read-only access (uncomment if you only need read access)
-- ALTER ROLE db_datareader ADD MEMBER [sp-dataplatform-sql];
-- PRINT 'Granted db_datareader role';

-- Option 2: Read and write access (uncomment if you need write access)
-- ALTER ROLE db_datawriter ADD MEMBER [sp-dataplatform-sql];
-- ALTER ROLE db_datareader ADD MEMBER [sp-dataplatform-sql];
-- PRINT 'Granted db_datawriter and db_datareader roles';

-- Option 3: Full database access (uncomment if you need full access)
ALTER ROLE db_owner ADD MEMBER [sp-dataplatform-sql];
PRINT 'Granted db_owner role';
GO

-- Verify the user was created
SELECT 
    name AS UserName,
    type_desc AS UserType,
    authentication_type_desc AS AuthenticationType
FROM sys.database_principals
WHERE name = 'sp-dataplatform-sql';
GO

-- List all roles assigned to the service principal
SELECT 
    dp.name AS UserName,
    r.name AS RoleName
FROM sys.database_role_members rm
JOIN sys.database_principals r ON rm.role_principal_id = r.principal_id
JOIN sys.database_principals dp ON rm.member_principal_id = dp.principal_id
WHERE dp.name = 'sp-dataplatform-sql';
GO

PRINT 'Service principal setup completed successfully!';
GO
