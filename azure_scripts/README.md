# Azure Scripts

This folder contains scripts and configuration for Azure deployment and SQL Database management.

## Setup

### 1. Create Python Virtual Environment

```bash
cd azure_scripts
python3 -m venv venv
source venv/bin/activate  # On macOS/Linux
# or
venv\Scripts\activate  # On Windows
```

### 2. Install Dependencies

**On macOS:**

First, install unixodbc (required for pyodbc):
```bash
brew install unixodbc
```

Then install Python packages:
```bash
pip install -r requirements.txt
```

**On Linux:**

Install unixodbc and ODBC Driver 18 for SQL Server:
```bash
# Ubuntu/Debian
sudo apt-get update
sudo apt-get install unixodbc unixodbc-dev
curl https://packages.microsoft.com/keys/microsoft.asc | sudo apt-key add -
curl https://packages.microsoft.com/config/ubuntu/$(lsb_release -rs)/prod.list | sudo tee /etc/apt/sources.list.d/mssql-release.list
sudo apt-get update
sudo ACCEPT_EULA=Y apt-get install -y msodbcsql18

# Then install Python packages
pip install -r requirements.txt
```

**On Windows:**

Download and install [ODBC Driver 18 for SQL Server](https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server)

Then install Python packages:
```bash
pip install -r requirements.txt
```

### 3. Configure Environment Variables

Copy the sample environment file and update with your Azure details:
```bash
cp _env_sample .env
# Edit .env with your Azure subscription and SQL Database details
```

## Files

- **`.env`** - Azure configuration (subscription, resource group, SQL Database, service principal)
- **`_env_sample`** - Template for environment variables
- **`setup-sql-service-principal.sql`** - SQL script to grant service principal database access
- **`test-sql-connection.py`** - Python script to test service principal connection
- **`SETUP_SERVICE_PRINCIPAL.md`** - Complete guide for setting up service principal
- **`requirements.txt`** - Python package dependencies

## Usage

### Test SQL Database Connection

```bash
source venv/bin/activate  # Activate virtual environment
python test-sql-connection.py
```

### Setup Service Principal

See `SETUP_SERVICE_PRINCIPAL.md` for detailed instructions.

## Troubleshooting

### pyodbc Import Error on macOS

If you get an error about missing `libodbc.2.dylib`, install unixodbc:
```bash
brew install unixodbc
```

### ODBC Driver Not Found

Make sure ODBC Driver 18 for SQL Server is installed:
- **macOS**: `brew install msodbcsql18` (if available) or download from Microsoft
- **Linux**: Follow installation instructions above
- **Windows**: Download from Microsoft website

### Check Available ODBC Drivers

```bash
source venv/bin/activate
python -c "import pyodbc; print([x for x in pyodbc.drivers() if 'SQL Server' in x])"
```

## Security Notes

- Never commit `.env` file to version control
- Service principal secrets should be rotated regularly
- Use Azure Key Vault for production deployments
