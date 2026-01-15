#!/usr/bin/env python3
"""
Test script to verify Service Principal connection to Azure SQL Database
"""
import os
import sys
from pathlib import Path

# Add parent directory to path to load .env
sys.path.insert(0, str(Path(__file__).parent))

try:
    from dotenv import load_dotenv
    load_dotenv('.env')
except ImportError:
    print("Warning: python-dotenv not installed. Loading environment variables manually...")
    # Fallback: read .env file manually
    env_file = Path(__file__).parent / '.env'
    if env_file.exists():
        with open(env_file) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    # Remove quotes if present
                    value = value.strip('"').strip("'")
                    os.environ[key.strip()] = value

# Get environment variables
server = os.getenv('AZURE_SQL_SERVER_FQDN', 'dataplatformpoc.database.windows.net')
database = os.getenv('AZURE_SQL_DATABASE_NAME', 'dataplatform')
port = os.getenv('AZURE_SQL_PORT', '1433')
client_id = os.getenv('AZURE_CLIENT_ID')
client_secret = os.getenv('AZURE_CLIENT_SECRET')
tenant_id = os.getenv('AZURE_TENANT_ID')

# Validate required variables
if not all([client_id, client_secret, tenant_id]):
    print("ERROR: Missing required environment variables:")
    print(f"  AZURE_CLIENT_ID: {'✓' if client_id else '✗'}")
    print(f"  AZURE_CLIENT_SECRET: {'✓' if client_secret else '✗'}")
    print(f"  AZURE_TENANT_ID: {'✓' if tenant_id else '✗'}")
    sys.exit(1)

print("=" * 60)
print("Testing Service Principal Connection to Azure SQL Database")
print("=" * 60)
print(f"Server: {server}")
print(f"Database: {database}")
print(f"Port: {port}")
print(f"Client ID: {client_id}")
print(f"Tenant ID: {tenant_id}")
print("=" * 60)
print()

# Try to connect using pyodbc
driver_name = None
try:
    import pyodbc
    print("✓ pyodbc is installed")
    
    # Check for SQL Server ODBC drivers
    drivers = pyodbc.drivers()
    sql_drivers = [d for d in drivers if 'SQL Server' in d]
    
    if not sql_drivers:
        print("\n⚠ WARNING: No SQL Server ODBC drivers found!")
        print("\nYou need to install 'ODBC Driver 18 for SQL Server' to connect to Azure SQL Database.")
        print("\nTo install on macOS:")
        print("  1. Run: ./install-odbc-driver.sh")
        print("     OR")
        print("  2. Download from: https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server")
        print("     OR")
        print("  3. Install the package already downloaded: sudo installer -pkg /tmp/msodbcsql18.pkg -target /")
        print("\nAfter installation, restart your terminal and run this script again.")
        sys.exit(1)
    else:
        print(f"✓ Found SQL Server ODBC driver(s): {', '.join(sql_drivers)}")
        # Use ODBC Driver 18 if available, otherwise use the first one
        driver_name = next((d for d in sql_drivers if '18' in d), sql_drivers[0])
        print(f"  Using: {driver_name}")
        
except ImportError:
    print("✗ pyodbc is not installed")
    print("\nTo install pyodbc, run:")
    print("  source venv/bin/activate")
    print("  pip install pyodbc")
    print("\nOn macOS, you also need unixodbc:")
    print("  brew install unixodbc")
    sys.exit(1)

# Build connection string using the detected driver
connection_string = (
    f"Driver={{{driver_name}}};"
    f"Server=tcp:{server},{port};"
    f"Database={database};"
    f"Uid={client_id};"
    f"Pwd={client_secret};"
    f"Encrypt=yes;"
    f"TrustServerCertificate=no;"
    f"Connection Timeout=30;"
    f"Authentication=ActiveDirectoryServicePrincipal"
)

print("Attempting to connect...")
print()

try:
    # Connect to database
    conn = pyodbc.connect(connection_string, timeout=30)
    print("✓ Connection successful!")
    print()
    
    # Create cursor
    cursor = conn.cursor()
    
    # Test query 1: Get SQL Server version
    print("Test 1: Getting SQL Server version...")
    cursor.execute("SELECT @@VERSION")
    version = cursor.fetchone()[0]
    print(f"✓ SQL Server Version: {version.split(chr(10))[0]}")
    print()
    
    # Test query 2: Get current user
    print("Test 2: Getting current user...")
    cursor.execute("SELECT SYSTEM_USER, USER_NAME()")
    row = cursor.fetchone()
    print(f"✓ System User: {row[0]}")
    print(f"✓ Database User: {row[1]}")
    print()
    
    # Test query 3: Check database roles
    print("Test 3: Checking database roles...")
    cursor.execute("""
        SELECT 
            r.name AS RoleName
        FROM sys.database_role_members rm
        JOIN sys.database_principals r ON rm.role_principal_id = r.principal_id
        JOIN sys.database_principals dp ON rm.member_principal_id = dp.principal_id
        WHERE dp.name = SYSTEM_USER
    """)
    roles = cursor.fetchall()
    if roles:
        print(f"✓ Database Roles: {', '.join([r[0] for r in roles])}")
    else:
        print("  No roles assigned")
    print()
    
    # Test query 4: List tables (if user has permission)
    print("Test 4: Listing tables...")
    try:
        cursor.execute("""
            SELECT TABLE_SCHEMA, TABLE_NAME 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_TYPE = 'BASE TABLE'
            ORDER BY TABLE_SCHEMA, TABLE_NAME
        """)
        tables = cursor.fetchall()
        if tables:
            print(f"✓ Found {len(tables)} tables:")
            for schema, table in tables[:10]:  # Show first 10
                print(f"  - {schema}.{table}")
            if len(tables) > 10:
                print(f"  ... and {len(tables) - 10} more")
        else:
            print("  No tables found")
    except Exception as e:
        print(f"  ⚠ Could not list tables: {e}")
    print()
    
    # Close connection
    cursor.close()
    conn.close()
    
    print("=" * 60)
    print("✓ All tests passed! Service Principal connection is working.")
    print("=" * 60)
    
except pyodbc.Error as e:
    print(f"✗ Connection failed!")
    print(f"Error: {e}")
    print()
    print("Troubleshooting:")
    print("1. Verify the SQL script was executed successfully")
    print("2. Check that the service principal has the correct permissions")
    print("3. Ensure ODBC Driver 18 for SQL Server is installed")
    print("4. Verify the credentials in .env file are correct")
    sys.exit(1)
except Exception as e:
    print(f"✗ Unexpected error: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
