-- SQL Script to grant Service Principal access to Azure SQL Database (source_data)
-- Run this script as the Entra ID admin (bksdrodrigo@outlook.com)
-- Connect to the database: source_data

-- Service Principal Details:
-- Display Name: Gture Brigeston source_data
-- Note: Update App ID and Object ID below with values from Azure Portal

USE [source_data];
GO

-- Create a contained database user for the service principal
-- Using the App ID (Client ID) of the service principal
IF NOT EXISTS (SELECT * FROM sys.database_principals WHERE name = 'Gture Brigeston source_data')
BEGIN
    CREATE USER [Gture Brigeston source_data] FROM EXTERNAL PROVIDER;
    PRINT 'Service principal user created successfully';
END
ELSE
BEGIN
    PRINT 'Service principal user already exists';
END
GO

-- Grant database roles based on your requirements
-- Option 1: Read-only access (uncomment if you only need read access)
-- ALTER ROLE db_datareader ADD MEMBER [Gture Brigeston source_data];
-- PRINT 'Granted db_datareader role';

-- Option 2: Read and write access (uncomment if you need write access)
-- ALTER ROLE db_datawriter ADD MEMBER [Gture Brigeston source_data];
-- ALTER ROLE db_datareader ADD MEMBER [Gture Brigeston source_data];
-- PRINT 'Granted db_datawriter and db_datareader roles';

-- Option 3: Full database access (uncomment if you need full access)
ALTER ROLE db_owner ADD MEMBER [Gture Brigeston source_data];
PRINT 'Granted db_owner role';
GO

-- Verify the user was created
SELECT 
    name AS UserName,
    type_desc AS UserType,
    authentication_type_desc AS AuthenticationType
FROM sys.database_principals
WHERE name = 'Gture Brigeston source_data';
GO

-- List all roles assigned to the service principal
SELECT 
    dp.name AS UserName,
    r.name AS RoleName
FROM sys.database_role_members rm
JOIN sys.database_principals r ON rm.role_principal_id = r.principal_id
JOIN sys.database_principals dp ON rm.member_principal_id = dp.principal_id
WHERE dp.name = 'Gture Brigeston source_data';
GO

PRINT 'Service principal setup completed successfully!';
GO
