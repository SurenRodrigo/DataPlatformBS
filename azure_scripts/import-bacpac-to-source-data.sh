#!/usr/bin/env bash
# Script to import BACPAC file into Azure SQL Database (source_data)
# This script creates a temporary storage account, uploads the BACPAC, imports it, and cleans up
#
# IMPORTANT: This script uses Entra ID admin credentials for the import operation.
# Regular database connections (as in test-sql-connection-source-data.py) use service principal,
# but BACPAC imports require server-level admin access to create/overwrite databases.
# The service principal (sp-source-data-sql) is used for regular queries, not for imports.

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

# Validate required variables
if [ -z "$AZURE_SUBSCRIPTION_ID" ] || [ -z "$AZURE_RESOURCE_GROUP_NAME" ] || [ -z "$AZURE_LOCATION" ]; then
    print_error "Missing required environment variables in .env"
    exit 1
fi

# Set variables
# Allow overriding BACPAC file via env var
BACPAC_FILE="${BACPAC_FILE:-WideWorldImportersDW-Standard.bacpac}"
# Generate unique storage account name (must be lowercase, 3-24 chars, alphanumeric)
TIMESTAMP=$(date +%s)
STORAGE_ACCOUNT_NAME="bacpac$(echo "$TIMESTAMP" | tail -c 6)"  # Unique name with timestamp
# Ensure storage account name is valid (lowercase, max 24 chars)
STORAGE_ACCOUNT_NAME=$(echo "$STORAGE_ACCOUNT_NAME" | tr '[:upper:]' '[:lower:]' | cut -c1-24)
CONTAINER_NAME="bacpac-imports"
BLOB_NAME="$(basename "$BACPAC_FILE")"
SQL_SERVER_NAME="${AZURE_SQL_SERVER_NAME:-dataplatformpoc}"
DATABASE_NAME="${AZURE_SOURCE_DATA_DATABASE_NAME:-source_data}"
ADMIN_USER_ENTRA="${AZURE_SQL_ADMIN_USER_ENTRA:-}"
ADMIN_USER_SQL="${AZURE_SQL_ADMIN_USER_SQL:-}"

# Validate critical variables
if [ -z "$SQL_SERVER_NAME" ] || [ -z "$DATABASE_NAME" ]; then
    print_error "Missing required SQL configuration variables"
    print_error "SQL_SERVER_NAME: ${SQL_SERVER_NAME:-NOT SET}"
    print_error "DATABASE_NAME: ${DATABASE_NAME:-NOT SET}"
    exit 1
fi

print_status "=========================================="
print_status "BACPAC Import to Azure SQL Database"
print_status "=========================================="
print_status "BACPAC File: $BACPAC_FILE"
print_status "Target Database: $DATABASE_NAME"
print_status "SQL Server: $SQL_SERVER_NAME"
print_status "Resource Group: $AZURE_RESOURCE_GROUP_NAME"
print_status "=========================================="
echo ""

# Check if BACPAC file exists
if [ ! -f "$BACPAC_FILE" ]; then
    print_error "BACPAC file not found: $BACPAC_FILE"
    exit 1
fi

print_success "BACPAC file found: $(du -h "$BACPAC_FILE" | cut -f1)"
echo ""

# Check if Azure CLI is installed
if ! command -v az >/dev/null 2>&1; then
    print_error "Azure CLI (az) is not installed"
    print_error "Install it from: https://learn.microsoft.com/en-us/cli/azure/install-azure-cli"
    exit 1
fi

# Check if logged in to Azure
print_status "Checking Azure CLI login status..."
if ! az account show >/dev/null 2>&1; then
    print_error "Not logged in to Azure CLI"
    print_error "Please run: az login"
    exit 1
fi
print_success "Azure CLI is logged in"

# Validate subscription exists in current login
print_status "Validating subscription access..."
# Normalize values to avoid CRLF or whitespace issues
SUBSCRIPTION_ID_TO_USE=$(echo "$AZURE_SUBSCRIPTION_ID" | tr -d '\r' | xargs)
AZURE_SUBSCRIPTION_NAME=$(echo "$AZURE_SUBSCRIPTION_NAME" | tr -d '\r' | xargs)
AZURE_RESOURCE_GROUP_NAME=$(echo "$AZURE_RESOURCE_GROUP_NAME" | tr -d '\r' | xargs)
AZURE_LOCATION=$(echo "$AZURE_LOCATION" | tr -d '\r' | xargs)
SQL_SERVER_NAME=$(echo "$SQL_SERVER_NAME" | tr -d '\r' | xargs)
DATABASE_NAME=$(echo "$DATABASE_NAME" | tr -d '\r' | xargs)
ADMIN_USER_ENTRA=$(echo "$ADMIN_USER_ENTRA" | tr -d '\r' | xargs)
ADMIN_USER_SQL=$(echo "$ADMIN_USER_SQL" | tr -d '\r' | xargs)

print_status "Using subscription ID: ${SUBSCRIPTION_ID_TO_USE}"

SUBSCRIPTION_MATCH_COUNT=$(az account list --query "[?id=='$AZURE_SUBSCRIPTION_ID'] | length(@)" -o tsv 2>/dev/null || echo "0")
if [ "$SUBSCRIPTION_MATCH_COUNT" != "1" ]; then
    print_warning "Subscription ID not found in current Azure login: $AZURE_SUBSCRIPTION_ID"
    if [ -n "$AZURE_SUBSCRIPTION_NAME" ]; then
        print_status "Trying subscription name: $AZURE_SUBSCRIPTION_NAME"
        SUBSCRIPTION_NAME_MATCH=$(az account list --query "[?name=='$AZURE_SUBSCRIPTION_NAME'] | length(@)" -o tsv 2>/dev/null || echo "0")
        if [ "$SUBSCRIPTION_NAME_MATCH" = "1" ]; then
            SUBSCRIPTION_ID_TO_USE=$(az account list --query "[?name=='$AZURE_SUBSCRIPTION_NAME'].id | [0]" -o tsv)
            print_success "Found subscription by name. Using ID: $SUBSCRIPTION_ID_TO_USE"
        else
            print_error "Subscription name not found: $AZURE_SUBSCRIPTION_NAME"
            print_error "Available subscriptions:"
            az account list --query "[].{Name:name, ID:id}" -o table
            print_error "Please run: az login --tenant $AZURE_TENANT_ID"
            exit 1
        fi
    else
        print_error "No AZURE_SUBSCRIPTION_NAME provided as fallback."
        print_error "Available subscriptions:"
        az account list --query "[].{Name:name, ID:id}" -o table
        print_error "Please run: az login --tenant $AZURE_TENANT_ID"
        exit 1
    fi
fi

# Set Azure subscription
print_status "Setting Azure subscription..."
if ! az account set --subscription "$SUBSCRIPTION_ID_TO_USE" >/dev/null 2>&1; then
    print_error "Failed to set subscription: $SUBSCRIPTION_ID_TO_USE"
    print_error "Available subscriptions:"
    az account list --query "[].{Name:name, ID:id}" -o table
    exit 1
fi
print_success "Subscription set to: $SUBSCRIPTION_ID_TO_USE"
echo ""

# Create storage account
print_status "Creating temporary storage account: $STORAGE_ACCOUNT_NAME"
if az storage account create \
    --resource-group "$AZURE_RESOURCE_GROUP_NAME" \
    --name "$STORAGE_ACCOUNT_NAME" \
    --location "$AZURE_LOCATION" \
    --sku Standard_LRS \
    --kind StorageV2 \
    --subscription "$SUBSCRIPTION_ID_TO_USE" \
    --output none 2>&1; then
    print_success "Storage account created"
else
    print_error "Failed to create storage account"
    exit 1
fi
echo ""

# Get storage account key
print_status "Retrieving storage account key..."
STORAGE_KEY=$(az storage account keys list \
    --resource-group "$AZURE_RESOURCE_GROUP_NAME" \
    --account-name "$STORAGE_ACCOUNT_NAME" \
    --subscription "$SUBSCRIPTION_ID_TO_USE" \
    --query "[0].value" -o tsv)

if [ -z "$STORAGE_KEY" ]; then
    print_error "Failed to retrieve storage account key"
    exit 1
fi
print_success "Storage account key retrieved"
echo ""

# Create container
print_status "Creating blob container: $CONTAINER_NAME"
if az storage container create \
    --account-name "$STORAGE_ACCOUNT_NAME" \
    --account-key "$STORAGE_KEY" \
    --name "$CONTAINER_NAME" \
    --output none 2>&1; then
    print_success "Container created"
else
    print_error "Failed to create container"
    exit 1
fi
echo ""

# Upload BACPAC file
print_status "Uploading BACPAC file to blob storage..."
print_status "This may take a few minutes for large files..."
if az storage blob upload \
    --account-name "$STORAGE_ACCOUNT_NAME" \
    --account-key "$STORAGE_KEY" \
    --container-name "$CONTAINER_NAME" \
    --name "$BLOB_NAME" \
    --file "$BACPAC_FILE" \
    --output none 2>&1; then
    print_success "BACPAC file uploaded"
else
    print_error "Failed to upload BACPAC file"
    exit 1
fi
echo ""

# Get storage account connection string
STORAGE_CONNECTION_STRING=$(az storage account show-connection-string \
    --resource-group "$AZURE_RESOURCE_GROUP_NAME" \
    --name "$STORAGE_ACCOUNT_NAME" \
    --subscription "$SUBSCRIPTION_ID_TO_USE" \
    --query "connectionString" -o tsv)

# Import BACPAC to database
print_status "Importing BACPAC to database: $DATABASE_NAME"
print_status "This may take several minutes..."
print_warning "Note: This will overwrite the existing database if it contains data"
echo ""
print_status "Note: BACPAC imports require server-level admin access."
print_status "Regular connections use service principal (as in test-sql-connection-source-data.py),"
print_status "but imports need Entra ID admin credentials to create/overwrite the database."
echo ""

# Check if we're using Entra ID authentication (no password needed)
# Azure CLI will use the currently logged-in user's Entra ID credentials
print_status "Entra ID admin user: ${ADMIN_USER_ENTRA:-NOT SET}"
print_status "SQL admin user: ${ADMIN_USER_SQL:-NOT SET}"
print_status "If you need to re-authenticate, run: az login"
echo ""

# Import using Azure CLI
# NOTE: az sql db import requires administratorLoginPassword (SQL auth).
print_status "Starting import operation..."
if [ -z "$AZURE_SQL_ADMIN_PASSWORD" ] || [ -z "$ADMIN_USER_SQL" ]; then
    print_error "AZURE_SQL_ADMIN_USER_SQL or AZURE_SQL_ADMIN_PASSWORD is not set in .env"
    print_error "az sql db import requires SQL admin username and password."
    print_error "Recommendation: set the SQL admin credentials or use SqlPackage:"
    print_error "  ./import-bacpac-sqlpackage.sh"
    exit 1
fi

# Import using Azure CLI
print_status "Executing import command..."
# Temporarily disable exit-on-error to capture command output
set +e
IMPORT_OUTPUT=$(az sql db import \
    --resource-group "$AZURE_RESOURCE_GROUP_NAME" \
    --server "$SQL_SERVER_NAME" \
    --name "$DATABASE_NAME" \
    --admin-user "$ADMIN_USER_SQL" \
    --admin-password "$AZURE_SQL_ADMIN_PASSWORD" \
    --subscription "$SUBSCRIPTION_ID_TO_USE" \
    --storage-key-type StorageAccessKey \
    --storage-key "$STORAGE_KEY" \
    --storage-uri "https://${STORAGE_ACCOUNT_NAME}.blob.core.windows.net/${CONTAINER_NAME}/${BLOB_NAME}" \
    --output json 2>&1)
IMPORT_EXIT_CODE=$?
set -e

if [ $IMPORT_EXIT_CODE -eq 0 ]; then
    print_success "Import operation started successfully"
    print_status "Import is running in the background. This may take 10-30 minutes depending on database size."
    echo ""
    print_status "You can check the import status with:"
    echo "  az sql db show --resource-group $AZURE_RESOURCE_GROUP_NAME --server $SQL_SERVER_NAME --name $DATABASE_NAME --query status"
    echo ""
else
    print_error "Failed to start import operation"
    echo ""
    echo "Error output:"
    echo "$IMPORT_OUTPUT"
    echo ""
    print_status "Troubleshooting:"
    print_status "1. Verify you're logged in: az login"
    print_status "2. Check Entra ID admin permissions on SQL Server"
    print_status "3. Try using SqlPackage method: ./import-bacpac-sqlpackage.sh"
    exit 1
fi

# Wait for import to complete (optional)
echo -n "Do you want to wait for the import to complete? (y/n) "
read -n 1 -r REPLY
echo ""
if [[ "$REPLY" =~ ^[Yy]$ ]]; then
    print_status "Waiting for import to complete..."
    print_status "This may take 10-30 minutes. Press Ctrl+C to cancel and check status manually."
    
    while true; do
        STATUS=$(az sql db show \
            --resource-group "$AZURE_RESOURCE_GROUP_NAME" \
            --server "$SQL_SERVER_NAME" \
            --name "$DATABASE_NAME" \
            --subscription "$SUBSCRIPTION_ID_TO_USE" \
            --query "status" -o tsv 2>/dev/null || echo "Unknown")
        
        if [ "$STATUS" == "Online" ]; then
            print_success "Database is online! Import may have completed."
            break
        fi
        
        print_status "Database status: $STATUS (waiting...)"
        sleep 30
    done
fi

# Cleanup: Delete storage account and container
echo -n "Do you want to delete the temporary storage account? (y/n) "
read -n 1 -r REPLY
echo ""
if [[ "$REPLY" =~ ^[Yy]$ ]]; then
    print_status "Deleting temporary storage account..."
    az storage account delete \
        --resource-group "$AZURE_RESOURCE_GROUP_NAME" \
        --name "$STORAGE_ACCOUNT_NAME" \
        --subscription "$SUBSCRIPTION_ID_TO_USE" \
        --yes \
        --output none 2>&1
    print_success "Storage account deleted"
else
    print_warning "Storage account '$STORAGE_ACCOUNT_NAME' was not deleted."
    print_warning "You can delete it manually later with:"
    echo "  az storage account delete --resource-group $AZURE_RESOURCE_GROUP_NAME --name $STORAGE_ACCOUNT_NAME --subscription $SUBSCRIPTION_ID_TO_USE --yes"
fi

echo ""
print_success "=========================================="
print_success "BACPAC Import Process Completed"
print_success "=========================================="
print_status "Database: $DATABASE_NAME"
print_status "Server: $SQL_SERVER_NAME"
print_status ""
print_status "You can now test the connection with:"
print_status "  python test-sql-connection-source-data.py"
