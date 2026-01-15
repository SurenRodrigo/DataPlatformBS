# Service Principal Setup for source_data Database

This guide documents the setup of a service principal for accessing the `source_data` database on the same Azure SQL Server.

## ✅ Completed Steps

### Step 1: Service Principal Created
- **Display Name**: `sp-source-data-sql`
- **App ID (Client ID)**: `6db2d6ae-02e6-467a-8195-707af94a0c89`
- **Object ID**: `189c06e1-b17c-4f75-aeba-aa3a283bc0eb`
- **Tenant ID**: `4a758f28-8613-4bc8-b9f2-bb78b961f784`
- **Secret**: Stored in `.env` file (AZURE_SOURCE_DATA_CLIENT_SECRET)

### Step 2: Service Principal Credentials Added to .env
The service principal credentials have been added to `azure_scripts/.env`:
- `AZURE_SOURCE_DATA_CLIENT_ID`
- `AZURE_SOURCE_DATA_CLIENT_SECRET`
- `AZURE_SOURCE_DATA_TENANT_ID`
- `AZURE_SOURCE_DATA_DATABASE_NAME`

## 🔧 Next Steps: Grant Database Access

### Step 3: Run SQL Script to Grant Database Access

You need to run the SQL script `setup-sql-service-principal-source-data.sql` to grant the service principal access to your `source_data` database.

#### Option A: Using Azure Portal Query Editor

1. Go to Azure Portal → SQL Database → `source_data`
2. Click on **"Query editor (preview)"** in the left menu
3. Sign in with your Entra ID admin account (`bksdrodrigo@outlook.com`)
4. Copy and paste the contents of `setup-sql-service-principal-source-data.sql`
5. Execute the script
6. Verify the output shows the service principal user was created

#### Option B: Using Azure Data Studio or SQL Server Management Studio

1. Connect to your SQL server:
   - **Server**: `dataplatformpoc.database.windows.net`
   - **Authentication**: Azure Active Directory - Universal with MFA
   - **Username**: `bksdrodrigo@outlook.com`
   - **Database**: `source_data`

2. Open and execute `setup-sql-service-principal-source-data.sql`

3. Verify the output shows:
   - Service principal user created
   - Role assigned (db_owner, db_datareader, or db_datawriter)

#### Option C: Using Azure CLI (sqlcmd)

```bash
# Install sqlcmd if not already installed
# macOS: brew install mssql-tools
# Linux: Follow Microsoft docs

# Connect and run the script
sqlcmd -S dataplatformpoc.database.windows.net \
  -d source_data \
  -U bksdrodrigo@outlook.com \
  -G \
  -i setup-sql-service-principal-source-data.sql
```

## 🔍 Verify Service Principal Access

After running the SQL script, you can verify the setup:

### Check Database User

```sql
USE [source_data];
GO

-- Check if user exists
SELECT 
    name AS UserName,
    type_desc AS UserType,
    authentication_type_desc AS AuthenticationType
FROM sys.database_principals
WHERE name = 'sp-source-data-sql';
GO

-- Check assigned roles
SELECT 
    dp.name AS UserName,
    r.name AS RoleName
FROM sys.database_role_members rm
JOIN sys.database_principals r ON rm.role_principal_id = r.principal_id
JOIN sys.database_principals dp ON rm.member_principal_id = dp.principal_id
WHERE dp.name = 'sp-source-data-sql';
GO
```

## 🧪 Test Connection

After running the SQL script, test the connection:

```bash
cd azure_scripts
source venv/bin/activate
python test-sql-connection-source-data.py
```

## 🔐 Connection String for Service Principal

Once the database user is created, you can use this connection string format:

**ODBC Connection String:**
```
Driver={ODBC Driver 18 for SQL Server};Server=tcp:dataplatformpoc.database.windows.net,1433;Database=source_data;Uid=6db2d6ae-02e6-467a-8195-707af94a0c89;Pwd={AZURE_SOURCE_DATA_CLIENT_SECRET};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;Authentication=ActiveDirectoryServicePrincipal
```

**Python (pyodbc) Example:**
```python
import pyodbc
import os
from dotenv import load_dotenv

load_dotenv('azure_scripts/.env')

connection_string = (
    f"Driver={{ODBC Driver 18 for SQL Server}};"
    f"Server=tcp:{os.getenv('AZURE_SQL_SERVER_FQDN')},{os.getenv('AZURE_SQL_PORT')};"
    f"Database={os.getenv('AZURE_SOURCE_DATA_DATABASE_NAME')};"
    f"Uid={os.getenv('AZURE_SOURCE_DATA_CLIENT_ID')};"
    f"Pwd={os.getenv('AZURE_SOURCE_DATA_CLIENT_SECRET')};"
    f"Encrypt=yes;"
    f"TrustServerCertificate=no;"
    f"Connection Timeout=30;"
    f"Authentication=ActiveDirectoryServicePrincipal"
)

conn = pyodbc.connect(connection_string)
```

## 📝 Important Notes

1. **Security**: The service principal secret is stored in `.env` file. Never commit this to version control.

2. **Database Roles**: The script grants `db_owner` role by default. You can modify the script to grant:
   - `db_datareader` - Read-only access
   - `db_datawriter` + `db_datareader` - Read and write access
   - `db_owner` - Full database access (current setting)

3. **Secret Expiration**: The service principal secret expires in 2 years. You'll need to rotate it before expiration.

4. **ODBC Driver**: Make sure you have ODBC Driver 18 for SQL Server installed on your system.

## 🔄 Rotating Service Principal Secret

If you need to rotate the secret:

```bash
# Generate new secret
az ad sp credential reset --id 6db2d6ae-02e6-467a-8195-707af94a0c89 --years 2

# Update .env file with new secret (AZURE_SOURCE_DATA_CLIENT_SECRET)
```

## 📊 Summary

- **Database**: `source_data`
- **Server**: `dataplatformpoc.database.windows.net`
- **Service Principal**: `sp-source-data-sql`
- **Test Script**: `test-sql-connection-source-data.py`
- **SQL Setup Script**: `setup-sql-service-principal-source-data.sql`

## 🔗 Related Files

- `.env` - Contains service principal credentials
- `setup-sql-service-principal-source-data.sql` - SQL script to grant database access
- `test-sql-connection-source-data.py` - Python test script
- `SETUP_SERVICE_PRINCIPAL.md` - Guide for the main dataplatform database
