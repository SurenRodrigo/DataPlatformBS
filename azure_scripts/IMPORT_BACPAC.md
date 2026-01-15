# Importing BACPAC to source_data Database

This guide explains how to import the `WideWorldImporters-Standard.bacpac` file into the `source_data` Azure SQL Database.

## Authentication Methods

### Service Principal (Regular Connections)
- **Used for**: Regular database queries and connections
- **Script**: `test-sql-connection-source-data.py`
- **Service Principal**: `sp-source-data-sql`
- **Permissions**: Database-level (db_owner, db_datareader, etc.)
- **Purpose**: Application connections, data queries, ETL operations

### Entra ID Admin (Database Imports)
- **Used for**: BACPAC imports, database creation/overwrite operations
- **Scripts**: `import-bacpac-to-source-data.sh` or `import-bacpac-sqlpackage.sh`
- **User**: `bksdrodrigo@outlook.com` (Entra ID admin)
- **Permissions**: Server-level admin access
- **Purpose**: Database schema imports, initial setup, administrative operations

**Why different authentication?**
- BACPAC imports require server-level permissions to create/overwrite databases
- Service principals typically have database-level permissions only
- Regular operations use service principal for security and least-privilege access

## Import Methods

### Method 1: Azure CLI (Requires SQL admin password)

Uses Azure CLI with blob storage:

```bash
cd azure_scripts
# Use the correct BACPAC file
BACPAC_FILE="WideWorldImporters-Standard.bacpac" ./import-bacpac-to-source-data.sh
```

**What it does:**
1. Creates a temporary Azure Storage account
2. Uploads the BACPAC file to blob storage
3. Imports the BACPAC using `az sql db import`
4. Optionally cleans up the storage account

**Requirements:**
- Azure CLI installed and logged in (`az login`)
- Entra ID admin access to SQL Server
- Sufficient permissions to create storage accounts
- SQL admin password set in `.env` as `AZURE_SQL_ADMIN_PASSWORD` (required by `az sql db import`)

### Method 2: SqlPackage (Recommended if no SQL admin password)

Uses SqlPackage utility for direct import:

```bash
cd azure_scripts
./import-bacpac-sqlpackage.sh
```

**What it does:**
1. Downloads SqlPackage if not installed
2. Imports BACPAC directly from local file
3. Uses Entra ID authentication

**Requirements:**
- SqlPackage utility (script will download if needed)
- Entra ID admin access to SQL Server
- Local BACPAC file

## Prerequisites

1. **BACPAC File**: `WideWorldImporters-Standard.bacpac` (58MB) in `azure_scripts/` folder ✓
2. **Azure CLI**: Logged in with Entra ID admin account
   ```bash
   az login
   az account show  # Verify you're logged in
   ```
3. **Environment Variables**: `.env` file configured with:
   - `AZURE_SUBSCRIPTION_ID`
   - `AZURE_RESOURCE_GROUP_NAME`
   - `AZURE_SQL_SERVER_NAME`
   - `AZURE_SOURCE_DATA_DATABASE_NAME`
   - `AZURE_SQL_ADMIN_USER_SQL` (or `AZURE_SQL_ADMIN_USER_ENTRA` for SqlPackage Entra ID auth)

## Import Process

### Step 1: Run Import Script

Choose one of the methods above. The script will:
- Validate prerequisites
- Create temporary storage (Method 1) or use local file (Method 2)
- Start the import operation
- Monitor progress (optional)

### Step 2: Wait for Import

Import time depends on database size:
- **Small databases (< 1GB)**: 5-10 minutes
- **Medium databases (1-10GB)**: 10-30 minutes
- **Large databases (> 10GB)**: 30+ minutes

The script will show progress or you can check manually:
```bash
az sql db show \
  --resource-group data_platform_mssql_test \
  --server dataplatformpoc \
  --name source_data \
  --query status
```

### Step 3: Verify Import

After import completes, test the connection:

```bash
cd azure_scripts
source venv/bin/activate
python test-sql-connection-source-data.py
```

The test script will:
- Connect using service principal
- List tables in the database
- Verify data is accessible

## Troubleshooting

### Import Fails with Authentication Error

**Problem**: `az sql db import` requires password authentication

**Solution**: 
1. Use SqlPackage method instead (better Entra ID support)
2. Or ensure you're logged in: `az login`
3. Or use SQL admin password if available

### Import Takes Too Long

**Problem**: Import seems stuck

**Solution**:
- Check database status: `az sql db show --name source_data --query status`
- Check import operation: Azure Portal → SQL Database → source_data → Activity log
- Large databases can take 30+ minutes

### Storage Account Creation Fails

**Problem**: Cannot create temporary storage account

**Solution**:
- Check resource group permissions
- Verify subscription has available quota
- Use SqlPackage method (doesn't require storage account)

### Service Principal Cannot Access Imported Data

**Problem**: After import, service principal can't query tables

**Solution**:
1. Verify service principal was granted access:
   ```sql
   USE [source_data];
   SELECT name FROM sys.database_principals WHERE name = 'sp-source-data-sql';
   ```
2. Re-run setup script if needed:
   ```sql
   -- Run setup-sql-service-principal-source-data.sql
   ```

## Files

- **`WideWorldImportersDW-Standard.bacpac`** - Database backup file to import
- **`import-bacpac-to-source-data.sh`** - Azure CLI import script
- **`import-bacpac-sqlpackage.sh`** - SqlPackage import script
- **`test-sql-connection-source-data.py`** - Connection test (uses service principal)

## Post-Import

After successful import:

1. **Verify Tables**: The database should contain WideWorldImportersDW tables
2. **Test Connection**: Run `test-sql-connection-source-data.py`
3. **Check Data**: Query tables to verify data was imported
4. **Service Principal**: Ensure service principal can access the data

## Summary

- **Regular Connections**: Use service principal (`test-sql-connection-source-data.py`)
- **Database Imports**: Use Entra ID admin (`import-bacpac-*.sh` scripts)
- **Both methods work together**: Admin for setup, service principal for operations
