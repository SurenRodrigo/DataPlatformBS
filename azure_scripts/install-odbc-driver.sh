#!/bin/bash
# Script to install Microsoft ODBC Driver 18 for SQL Server on macOS

set -e

echo "=========================================="
echo "Installing Microsoft ODBC Driver 18"
echo "=========================================="
echo ""

# Check if already installed
if odbcinst -q -d | grep -q "ODBC Driver 18 for SQL Server"; then
    echo "✓ ODBC Driver 18 for SQL Server is already installed"
    exit 0
fi

# Download the driver
echo "Downloading ODBC Driver 18 for SQL Server..."
DOWNLOAD_URL="https://go.microsoft.com/fwlink/?linkid=2249004"
PKG_FILE="/tmp/msodbcsql18.pkg"

curl -L -o "$PKG_FILE" "$DOWNLOAD_URL"

if [ ! -f "$PKG_FILE" ]; then
    echo "✗ Failed to download ODBC Driver"
    exit 1
fi

echo "✓ Download complete"
echo ""
echo "Installing ODBC Driver..."
echo "Note: This requires administrator privileges (sudo)"
echo ""

# Install the package
sudo installer -pkg "$PKG_FILE" -target /

# Clean up
rm -f "$PKG_FILE"

echo ""
echo "=========================================="
echo "Installation complete!"
echo "=========================================="
echo ""
echo "Verifying installation..."

# Verify installation
if odbcinst -q -d | grep -q "ODBC Driver 18 for SQL Server"; then
    echo "✓ ODBC Driver 18 for SQL Server is now installed"
    echo ""
    echo "Available drivers:"
    odbcinst -q -d
else
    echo "⚠ Installation completed, but driver not found in odbcinst"
    echo "You may need to restart your terminal or check the installation"
fi
