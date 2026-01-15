#!/bin/bash
# Alternative script to import BACPAC using SqlPackage utility
# This script downloads SqlPackage if needed and imports directly

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Load environment variables
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

if [ ! -f .env ]; then
    print_error ".env file not found in azure_scripts directory"
    exit 1
fi

source .env

# Set variables
BACPAC_FILE="WideWorldImportersDW-Standard.bacpac"
SQL_SERVER_NAME="$AZURE_SQL_SERVER_NAME"
DATABASE_NAME="$AZURE_SOURCE_DATA_DATABASE_NAME"
ADMIN_USER_ENTRA="$AZURE_SQL_ADMIN_USER_ENTRA"
ADMIN_USER_SQL="$AZURE_SQL_ADMIN_USER_SQL"
SQL_SERVER_FQDN="$AZURE_SQL_SERVER_FQDN"

print_status "=========================================="
print_status "BACPAC Import using SqlPackage"
print_status "=========================================="
print_status "BACPAC File: $BACPAC_FILE"
print_status "Target Database: $DATABASE_NAME"
print_status "SQL Server: $SQL_SERVER_FQDN"
print_status "=========================================="
echo ""

# Check if BACPAC file exists
if [ ! -f "$BACPAC_FILE" ]; then
    print_error "BACPAC file not found: $BACPAC_FILE"
    exit 1
fi

print_success "BACPAC file found: $(du -h "$BACPAC_FILE" | cut -f1)"
echo ""

# Check for SqlPackage
SQLPACKAGE_PATH=""
if command -v sqlpackage >/dev/null 2>&1; then
    SQLPACKAGE_PATH="sqlpackage"
    print_success "SqlPackage found in PATH"
elif [ -f "/usr/local/bin/sqlpackage" ]; then
    SQLPACKAGE_PATH="/usr/local/bin/sqlpackage"
    print_success "SqlPackage found at: $SQLPACKAGE_PATH"
elif [ -f "$SCRIPT_DIR/sqlpackage/sqlpackage" ]; then
    SQLPACKAGE_PATH="$SCRIPT_DIR/sqlpackage/sqlpackage"
    print_success "SqlPackage found at: $SQLPACKAGE_PATH"
else
    print_warning "SqlPackage not found. Downloading..."
    
    # Detect OS
    OS="$(uname -s)"
    ARCH="$(uname -m)"
    
    if [ "$OS" == "Darwin" ]; then
        if [ "$ARCH" == "arm64" ] || [ "$ARCH" == "aarch64" ]; then
            SQLPACKAGE_URL="https://aka.ms/sqlpackage-macos-arm64"
        else
            SQLPACKAGE_URL="https://aka.ms/sqlpackage-macos"
        fi
        SQLPACKAGE_ZIP="sqlpackage.zip"
    else
        print_error "Unsupported OS: $OS"
        print_error "Please install SqlPackage manually or use import-bacpac-to-source-data.sh instead"
        exit 1
    fi
    
    # Download SqlPackage
    print_status "Downloading SqlPackage..."
    curl -L -o "$SQLPACKAGE_ZIP" "$SQLPACKAGE_URL" || {
        print_error "Failed to download SqlPackage"
        exit 1
    }
    
    # Extract
    print_status "Extracting SqlPackage..."
    unzip -q "$SQLPACKAGE_ZIP" -d "$SCRIPT_DIR/sqlpackage" || {
        print_error "Failed to extract SqlPackage"
        exit 1
    }
    
    # Find sqlpackage binary
    SQLPACKAGE_PATH=$(find "$SCRIPT_DIR/sqlpackage" -name "sqlpackage" -type f | head -1)
    
    if [ -z "$SQLPACKAGE_PATH" ]; then
        print_error "Could not find sqlpackage binary after extraction"
        exit 1
    fi
    
    chmod +x "$SQLPACKAGE_PATH"
    print_success "SqlPackage downloaded and extracted"
    rm -f "$SQLPACKAGE_ZIP"
fi

echo ""

# Get admin password
if [ -z "$AZURE_SQL_ADMIN_PASSWORD" ]; then
    print_warning "SQL admin password not set in .env"
    print_status "You'll need to provide the password for: ${ADMIN_USER_SQL:-$ADMIN_USER_ENTRA}"
    read -sp "Enter SQL admin password: " SQL_PASSWORD
    echo ""
else
    SQL_PASSWORD="$AZURE_SQL_ADMIN_PASSWORD"
fi

# Build connection string
# Prefer SQL authentication if SQL admin user is set, otherwise fall back to Entra ID
if [ -n "$ADMIN_USER_SQL" ]; then
    print_status "Using SQL authentication for SqlPackage"
    CONNECTION_STRING="Server=$SQL_SERVER_FQDN,1433;Database=$DATABASE_NAME;User Id=$ADMIN_USER_SQL;Password=$SQL_PASSWORD;Encrypt=True;TrustServerCertificate=False"
else
    print_status "Using Entra ID authentication for SqlPackage"
    CONNECTION_STRING="Server=$SQL_SERVER_FQDN,1433;Database=$DATABASE_NAME;User Id=$ADMIN_USER_ENTRA;Password=$SQL_PASSWORD;Encrypt=True;TrustServerCertificate=False;Authentication=Active Directory Password"
fi

print_status "Starting BACPAC import..."
print_warning "Note: This will overwrite the existing database if it contains data"
print_status "This may take 10-30 minutes depending on database size..."
echo ""

# Import BACPAC
if "$SQLPACKAGE_PATH" /Action:Import \
    /SourceFile:"$BACPAC_FILE" \
    /TargetConnectionString:"$CONNECTION_STRING" \
    /p:CommandTimeout=0; then
    print_success "BACPAC import completed successfully!"
else
    print_error "BACPAC import failed"
    exit 1
fi

echo ""
print_success "=========================================="
print_success "BACPAC Import Completed"
print_success "=========================================="
print_status "Database: $DATABASE_NAME"
print_status "Server: $SQL_SERVER_FQDN"
print_status ""
print_status "You can now test the connection with:"
print_status "  python test-sql-connection-source-data.py"
