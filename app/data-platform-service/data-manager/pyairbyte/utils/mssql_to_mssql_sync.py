"""
MSSQL to MSSQL Query and Sync Utility

This module provides functionality to query a source MSSQL database and sync results
to a destination MSSQL table with checksum-based deduplication and auto-table creation.

Key Features:
- Query source MSSQL with custom queries
- Checksum-based deduplication (SHA256 hash of row values)
- Auto-create destination table with proper schema
- MERGE/UPSERT operations to handle inserts, updates, and duplicates
- Support for both SQL Authentication and Entra ID Service Principal
- Configurable record-level error handling

Author: Data Platform Team
"""

import os
import sys
import pyodbc
import pandas as pd
import numpy as np
import logging
import hashlib
import time
from typing import List, Optional, Dict, Any, Tuple
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# Pandas to MSSQL Type Mapping
# NOTE: For streaming/chunked data sync, we use GENEROUS types to avoid:
#   - Integer overflow (all ints → BIGINT)
#   - Precision loss (all floats → FLOAT)
#   - String truncation (all strings → NVARCHAR(MAX))
#   - NULL constraint violations (all columns → nullable)
# This "wide type" strategy ensures any source data fits in the destination.
PANDAS_TO_MSSQL_TYPE_MAP = {
    # All integer types → BIGINT (widest, handles any integer value)
    'int64': 'BIGINT',
    'Int64': 'BIGINT',
    'int32': 'BIGINT',  # Use BIGINT for safety (source INT might have larger values in other chunks)
    'Int32': 'BIGINT',
    'int16': 'BIGINT',  # Use BIGINT for safety
    'Int16': 'BIGINT',
    'int8': 'BIGINT',   # Use BIGINT for safety
    'Int8': 'BIGINT',
    'uint64': 'BIGINT',
    'UInt64': 'BIGINT',
    'uint32': 'BIGINT',
    'UInt32': 'BIGINT',
    'uint16': 'BIGINT',
    'UInt16': 'BIGINT',
    'uint8': 'BIGINT',
    'UInt8': 'BIGINT',
    
    # All float types → FLOAT (highest precision)
    'float64': 'FLOAT',
    'float32': 'FLOAT',  # Use FLOAT for safety (REAL has lower precision)
    'Float64': 'FLOAT',
    'Float32': 'FLOAT',
    
    # Boolean
    'bool': 'BIT',
    'boolean': 'BIT',
    
    # Date/time types → DATETIME2 (highest precision)
    'datetime64[ns]': 'DATETIME2',
    'datetime64': 'DATETIME2',
    'datetime64[ns, UTC]': 'DATETIME2',
    'timedelta64[ns]': 'NVARCHAR(100)',  # Store as string (no native MSSQL equivalent)
    'timedelta64': 'NVARCHAR(100)',
    
    # String types → NVARCHAR(MAX) (unlimited length)
    'object': 'NVARCHAR(MAX)',
    'string': 'NVARCHAR(MAX)',
    'category': 'NVARCHAR(MAX)',
    
    # Binary types
    'bytes': 'VARBINARY(MAX)',
}


def _get_mssql_connection(config: Dict[str, Any], max_retries: int = 3) -> Tuple[pyodbc.Connection, str]:
    """
    Establish connection to MSSQL database with retry logic and authentication detection.
    
    Supports:
    - Entra ID Service Principal Authentication (Azure SQL)
    - SQL Authentication (username/password)
    
    Args:
        config: Connection configuration dictionary with keys:
            Entra ID: client_id, client_secret, tenant_id, server, database, port (optional)
            SQL Auth: username, password, server, database, port (optional)
        max_retries: Maximum number of connection attempts (default: 3)
    
    Returns:
        Tuple of (pyodbc.Connection object, auth_method string)
    
    Raises:
        ConnectionError: If connection fails after all retries
        ValueError: If config is missing required keys
    """
    # Detect authentication method
    has_entra_id = all(k in config for k in ['client_id', 'client_secret', 'tenant_id'])
    has_sql_auth = all(k in config for k in ['username', 'password'])
    
    if not has_entra_id and not has_sql_auth:
        raise ValueError(
            "Invalid config: Must provide either Entra ID credentials "
            "(client_id, client_secret, tenant_id) or SQL Auth credentials (username, password)"
        )
    
    # Validate required fields
    if 'server' not in config or 'database' not in config:
        raise ValueError("Config must include 'server' and 'database'")
    
    server = config['server']
    database = config['database']
    port = config.get('port', '1433')
    
    # Try available ODBC drivers (prefer 18, fallback to 17)
    available_drivers = [d for d in pyodbc.drivers() if 'SQL Server' in d]
    if not available_drivers:
        raise ConnectionError("No SQL Server ODBC drivers found. Install 'ODBC Driver 18 for SQL Server'")
    
    # Prefer ODBC Driver 18
    driver = next((d for d in available_drivers if '18' in d), available_drivers[0])
    logger.info(f"Using ODBC driver: {driver}")
    
    # Build connection string based on authentication method
    if has_entra_id:
        # Entra ID Service Principal Authentication
        client_id = config['client_id']
        client_secret = config['client_secret']
        tenant_id = config['tenant_id']
        
        connection_string = (
            f"Driver={{{driver}}};"
            f"Server=tcp:{server},{port};"
            f"Database={database};"
            f"Uid={client_id};"
            f"Pwd={client_secret};"
            f"Encrypt=yes;"
            f"TrustServerCertificate=no;"
            f"Connection Timeout=30;"
            f"Authentication=ActiveDirectoryServicePrincipal"
        )
        auth_method = "service_principal"
        logger.info(f"Using Entra ID Service Principal authentication for {server}")
    
    else:
        # SQL Authentication
        username = config['username']
        password = config['password']
        
        connection_string = (
            f"DRIVER={{{driver}}};"
            f"SERVER={server};"
            f"DATABASE={database};"
            f"UID={username};"
            f"PWD={password};"
            f"TrustServerCertificate=yes;"
            f"Connection Timeout=30;"
        )
        auth_method = "sql_auth"
        logger.info(f"Using SQL Authentication for {server}")
    
    # Attempt connection with retry logic
    for attempt in range(1, max_retries + 1):
        try:
            logger.info(f"Connection attempt {attempt}/{max_retries} to {server}/{database}")
            conn = pyodbc.connect(connection_string, timeout=30)
            logger.info(f"Successfully connected to {server}/{database}")
            
            # Return connection and auth method as tuple
            return (conn, auth_method)
        
        except pyodbc.Error as e:
            logger.warning(f"Connection attempt {attempt} failed: {e}")
            if attempt < max_retries:
                wait_time = 5 * attempt  # Exponential backoff
                logger.info(f"Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                raise ConnectionError(
                    f"Failed to connect to {server}/{database} after {max_retries} attempts: {e}"
                )


def _generate_row_checksum(row: pd.Series, columns: List[str], skip_on_error: bool = False) -> Optional[str]:
    """
    Generate SHA256 checksum for a row based on specified columns.
    
    Args:
        row: Pandas Series representing a row
        columns: List of column names to include in checksum
        skip_on_error: If True, return None on error; if False, raise exception
    
    Returns:
        64-character hex string (SHA256 hash) or None if error and skip_on_error=True
    
    Raises:
        ValueError: If columns are invalid and skip_on_error=False
    """
    try:
        # Concatenate column values
        values = []
        for col in columns:
            if col not in row.index:
                raise ValueError(f"Column '{col}' not found in row")
            
            val = row[col]
            
            # Handle different data types
            if pd.isna(val):
                # NULL values represented as empty string
                values.append('')
            elif isinstance(val, (int, float, bool)):
                values.append(str(val))
            elif isinstance(val, (pd.Timestamp, datetime)):
                # Standardized datetime format
                values.append(val.isoformat() if not pd.isna(val) else '')
            elif isinstance(val, bytes):
                # Binary data as hex
                values.append(val.hex())
            else:
                # String or other types
                values.append(str(val))
        
        # Create concatenated string
        concatenated = '|'.join(values)
        
        # Generate SHA256 hash
        hash_obj = hashlib.sha256(concatenated.encode('utf-8'))
        checksum = hash_obj.hexdigest()
        
        return checksum
    
    except Exception as e:
        if skip_on_error:
            logger.warning(f"Checksum generation failed for row, skipping: {e}")
            return None
        else:
            raise ValueError(f"Checksum generation failed: {e}")


def _pandas_dtype_to_mssql(dtype: Any, series: pd.Series, use_max_for_strings: bool = True) -> str:
    """
    Map pandas dtype to MSSQL data type using GENEROUS/SAFE types for streaming.
    
    Strategy: Use the widest possible types to avoid issues with chunked data:
    - All integers → BIGINT (avoids overflow)
    - All floats → FLOAT (maximum precision)
    - All strings → NVARCHAR(MAX) (no length limit)
    - Decimals → DECIMAL(38, 10) (maximum precision and scale)
    - All columns → nullable (handled separately in schema inference)
    
    Args:
        dtype: Pandas dtype
        series: Pandas Series for analysis
        use_max_for_strings: If True, use NVARCHAR(MAX) for strings (default: True)
    
    Returns:
        MSSQL data type string (always a "safe" wide type)
    """
    dtype_str = str(dtype)
    
    # Check direct mapping first
    if dtype_str in PANDAS_TO_MSSQL_TYPE_MAP:
        return PANDAS_TO_MSSQL_TYPE_MAP[dtype_str]
    
    # Handle Decimal type (from Python decimal.Decimal or database DECIMAL/NUMERIC)
    if 'decimal' in dtype_str.lower():
        # Use maximum precision for safety
        return 'DECIMAL(38, 10)'
    
    # Handle nullable integer types (pandas >= 1.0)
    if dtype_str.startswith('Int') or dtype_str.startswith('UInt'):
        return 'BIGINT'
    
    # Handle nullable float types
    if dtype_str.startswith('Float'):
        return 'FLOAT'
    
    # Handle nullable boolean
    if dtype_str == 'boolean':
        return 'BIT'
    
    # Handle nullable string
    if dtype_str == 'string' or dtype_str.startswith('string'):
        return 'NVARCHAR(MAX)'
    
    # Handle datetime with timezone
    if 'datetime64' in dtype_str:
        return 'DATETIME2'
    
    # Handle object type (could be mixed types, strings, etc.)
    if dtype_str == 'object':
        # Check if it's actually numeric or date data stored as object
        non_null = series.dropna()
        if len(non_null) > 0:
            sample = non_null.iloc[0]
            if isinstance(sample, (int, np.integer)):
                return 'BIGINT'
            elif isinstance(sample, (float, np.floating)):
                return 'FLOAT'
            elif hasattr(sample, 'as_py'):  # PyArrow types
                return 'NVARCHAR(MAX)'
        return 'NVARCHAR(MAX)'
    
    # Fallback for any unknown types → NVARCHAR(MAX) can store anything as string
    logger.warning(f"Unknown dtype '{dtype_str}', defaulting to NVARCHAR(MAX)")
    return 'NVARCHAR(MAX)'


def _infer_mssql_schema_from_dataframe(df: pd.DataFrame, force_nullable: bool = True) -> List[Dict[str, Any]]:
    """
    Infer MSSQL table schema from pandas DataFrame.
    
    Args:
        df: Pandas DataFrame
        force_nullable: If True, make all columns nullable (required for streaming mode
                       where the first chunk may not have NULLs but later chunks do).
                       Default: True for safety in data sync scenarios.
    
    Returns:
        List of column metadata dictionaries with keys: name, type, nullable
    
    Note:
        For streaming/chunked processing, force_nullable=True is essential because
        the first chunk may not contain NULL values while later chunks do.
    """
    schema = []
    
    for col in df.columns:
        series = df[col]
        dtype = series.dtype
        
        # Determine MSSQL type
        mssql_type = _pandas_dtype_to_mssql(dtype, series)
        
        # For streaming data sync, always make columns nullable to handle
        # cases where later chunks have NULLs that weren't in the first chunk
        if force_nullable:
            nullable = True
        else:
            # Legacy behavior: infer from sample (NOT safe for streaming)
            nullable = series.isna().any()
        
        schema.append({
            'name': col,
            'type': mssql_type,
            'nullable': nullable
        })
    
    return schema


def _create_destination_table(
    conn: pyodbc.Connection,
    schema_name: str,
    table_name: str,
    columns: List[Dict[str, Any]]
) -> bool:
    """
    Create destination table in MSSQL with checksum and timestamp columns.
    
    Args:
        conn: pyodbc Connection
        schema_name: Schema name (e.g., "dbo", "reporting")
        table_name: Table name
        columns: List of column metadata from _infer_mssql_schema_from_dataframe
    
    Returns:
        True if table created successfully
    
    Raises:
        Exception: If table creation fails
    """
    cursor = conn.cursor()
    
    try:
        # Create schema if not exists
        logger.info(f"Creating schema [{schema_name}] if not exists")
        cursor.execute(f"""
            IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = '{schema_name}')
            BEGIN
                EXEC('CREATE SCHEMA [{schema_name}]')
            END
        """)
        
        # Build column definitions
        column_defs = []
        for col in columns:
            nullable = 'NULL' if col['nullable'] else 'NOT NULL'
            column_defs.append(f"[{col['name']}] {col['type']} {nullable}")
        
        # Add sync metadata columns
        column_defs.append("[_sync_checksum] VARCHAR(64) NULL")
        column_defs.append("[_sync_updated_at] DATETIME2 DEFAULT GETDATE()")
        
        # Create table
        create_table_sql = f"""
            CREATE TABLE [{schema_name}].[{table_name}] (
                {', '.join(column_defs)}
            )
        """
        
        logger.info(f"Creating table [{schema_name}].[{table_name}]")
        cursor.execute(create_table_sql)
        
        # Create index on checksum for performance
        index_name = f"IX_{table_name}_sync_checksum"
        create_index_sql = f"""
            CREATE INDEX [{index_name}] 
            ON [{schema_name}].[{table_name}] ([_sync_checksum])
        """
        
        logger.info(f"Creating index [{index_name}]")
        cursor.execute(create_index_sql)
        
        conn.commit()
        logger.info(f"Successfully created table [{schema_name}].[{table_name}] with checksum column")
        return True
    
    except Exception as e:
        conn.rollback()
        logger.error(f"Failed to create table [{schema_name}].[{table_name}]: {e}")
        raise
    finally:
        cursor.close()


def _get_table_schema(conn: pyodbc.Connection, schema_name: str, table_name: str) -> Optional[List[Dict[str, Any]]]:
    """
    Get table schema from MSSQL database.
    
    Args:
        conn: pyodbc Connection
        schema_name: Schema name
        table_name: Table name
    
    Returns:
        List of column metadata dicts with keys: name, type, nullable, max_length
        Returns None if table doesn't exist
    """
    cursor = conn.cursor()
    try:
        # First check if table exists
        exists_query = """
            SELECT COUNT(*) 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
        """
        cursor.execute(exists_query, (schema_name, table_name))
        count = cursor.fetchone()[0]
        
        if count == 0:
            return None
        
        # Get detailed schema information
        schema_query = """
            SELECT 
                COLUMN_NAME,
                DATA_TYPE,
                CHARACTER_MAXIMUM_LENGTH,
                IS_NULLABLE,
                NUMERIC_PRECISION,
                NUMERIC_SCALE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
            ORDER BY ORDINAL_POSITION
        """
        cursor.execute(schema_query, (schema_name, table_name))
        
        schema = []
        for row in cursor.fetchall():
            col_name = row[0]
            data_type = row[1].upper()
            max_length = row[2]
            is_nullable = row[3] == 'YES'
            precision = row[4]
            scale = row[5]
            
            # Build full type string with length/precision
            if data_type in ('NVARCHAR', 'VARCHAR', 'NCHAR', 'CHAR'):
                if max_length == -1:
                    full_type = f"{data_type}(MAX)"
                elif max_length:
                    full_type = f"{data_type}({max_length})"
                else:
                    full_type = data_type
            elif data_type in ('NUMERIC', 'DECIMAL') and precision:
                if scale:
                    full_type = f"{data_type}({precision},{scale})"
                else:
                    full_type = f"{data_type}({precision})"
            else:
                full_type = data_type
            
            schema.append({
                'name': col_name,
                'type': full_type,
                'nullable': is_nullable,
                'max_length': max_length
            })
        
        return schema
    
    finally:
        cursor.close()


def _validate_table_schema(
    existing_schema: List[Dict[str, Any]], 
    expected_schema: List[Dict[str, Any]]
) -> Dict[str, Any]:
    """
    Validate that existing table schema is compatible with expected schema.
    
    Args:
        existing_schema: Schema from existing table (_get_table_schema result)
        expected_schema: Expected schema from DataFrame (_infer_mssql_schema_from_dataframe result)
    
    Returns:
        Dictionary with validation results:
        {
            'is_valid': bool,
            'missing_columns': List[str],
            'extra_columns': List[str],
            'type_mismatches': List[Dict],
            'has_checksum_column': bool,
            'issues': List[str]
        }
    """
    # Build lookup dictionaries
    existing_cols = {col['name']: col for col in existing_schema}
    expected_cols = {col['name']: col for col in expected_schema}
    
    # Find missing and extra columns (excluding metadata columns)
    metadata_columns = ['_sync_checksum', '_sync_updated_at']
    
    expected_data_cols = {name for name in expected_cols.keys() if name not in metadata_columns}
    existing_data_cols = {name for name in existing_cols.keys() if name not in metadata_columns}
    
    missing_columns = list(expected_data_cols - existing_data_cols)
    extra_columns = list(existing_data_cols - expected_data_cols)
    
    # Check for type mismatches in common columns
    type_mismatches = []
    common_columns = expected_data_cols & existing_data_cols
    
    for col_name in common_columns:
        existing_type = existing_cols[col_name]['type']
        expected_type = expected_cols[col_name]['type']
        
        # Normalize types for comparison (handle variations)
        existing_normalized = existing_type.replace(' ', '').upper()
        expected_normalized = expected_type.replace(' ', '').upper()
        
        # Check if types are compatible (allow some flexibility)
        if not _are_types_compatible(existing_normalized, expected_normalized):
            type_mismatches.append({
                'column': col_name,
                'existing_type': existing_type,
                'expected_type': expected_type
            })
    
    # Check for checksum column
    has_checksum_column = '_sync_checksum' in existing_cols
    
    # Build issues list
    issues = []
    if missing_columns:
        issues.append(f"Missing columns in destination table: {', '.join(missing_columns)}")
    if extra_columns:
        issues.append(f"Extra columns in destination table (will be ignored): {', '.join(extra_columns)}")
    if type_mismatches:
        mismatch_details = [f"{m['column']} (existing: {m['existing_type']}, expected: {m['expected_type']})" 
                           for m in type_mismatches]
        issues.append(f"Type mismatches: {', '.join(mismatch_details)}")
    if not has_checksum_column:
        issues.append("Missing _sync_checksum column (required for deduplication)")
    
    is_valid = len(missing_columns) == 0 and len(type_mismatches) == 0 and has_checksum_column
    
    return {
        'is_valid': is_valid,
        'missing_columns': missing_columns,
        'extra_columns': extra_columns,
        'type_mismatches': type_mismatches,
        'has_checksum_column': has_checksum_column,
        'issues': issues
    }


def _are_types_compatible(type1: str, type2: str) -> bool:
    """
    Check if two MSSQL types are compatible for data insertion.
    
    Args:
        type1: First type (normalized uppercase)
        type2: Second type (normalized uppercase)
    
    Returns:
        True if types are compatible
    """
    # Exact match
    if type1 == type2:
        return True
    
    # Strip length/precision for base type comparison
    base_type1 = type1.split('(')[0]
    base_type2 = type2.split('(')[0]
    
    # Same base type
    if base_type1 == base_type2:
        # For string types, allow if existing is equal or larger
        if base_type1 in ('NVARCHAR', 'VARCHAR', 'NCHAR', 'CHAR'):
            # If either is MAX, they're compatible
            if 'MAX' in type1 or 'MAX' in type2:
                return True
            # Extract lengths
            try:
                len1 = int(type1.split('(')[1].split(')')[0]) if '(' in type1 else 0
                len2 = int(type2.split('(')[1].split(')')[0]) if '(' in type2 else 0
                return len1 >= len2  # Existing must be >= expected
            except:
                return True  # If we can't parse, assume compatible
        return True
    
    # Compatible numeric types
    numeric_groups = [
        {'TINYINT', 'SMALLINT', 'INT', 'BIGINT'},
        {'FLOAT', 'REAL', 'DOUBLE'},
        {'NUMERIC', 'DECIMAL', 'MONEY', 'SMALLMONEY'}
    ]
    
    for group in numeric_groups:
        if base_type1 in group and base_type2 in group:
            return True
    
    # Compatible date/time types
    datetime_types = {'DATETIME', 'DATETIME2', 'SMALLDATETIME', 'DATE', 'TIME'}
    if base_type1 in datetime_types and base_type2 in datetime_types:
        return True
    
    # VARCHAR and NVARCHAR are somewhat compatible (with potential data loss)
    if {base_type1, base_type2} <= {'VARCHAR', 'NVARCHAR'}:
        return True
    
    return False


def _add_checksum_column_to_table(
    conn: pyodbc.Connection,
    schema_name: str,
    table_name: str
) -> bool:
    """
    Add _sync_checksum column to existing table.
    
    Args:
        conn: pyodbc Connection
        schema_name: Schema name
        table_name: Table name
    
    Returns:
        True if column added successfully
    
    Raises:
        Exception: If adding column fails
    """
    cursor = conn.cursor()
    try:
        logger.info(f"Adding _sync_checksum column to [{schema_name}].[{table_name}]")
        
        # Add checksum column
        alter_sql = f"""
            ALTER TABLE [{schema_name}].[{table_name}]
            ADD [_sync_checksum] VARCHAR(64) NULL
        """
        cursor.execute(alter_sql)
        
        # Add updated_at column if not exists
        try:
            alter_sql2 = f"""
                ALTER TABLE [{schema_name}].[{table_name}]
                ADD [_sync_updated_at] DATETIME2 DEFAULT GETDATE()
            """
            cursor.execute(alter_sql2)
        except:
            # Column might already exist
            pass
        
        # Create index on checksum
        index_name = f"IX_{table_name}_sync_checksum"
        create_index_sql = f"""
            CREATE INDEX [{index_name}] 
            ON [{schema_name}].[{table_name}] ([_sync_checksum])
        """
        try:
            cursor.execute(create_index_sql)
        except:
            # Index might already exist
            pass
        
        conn.commit()
        logger.info(f"Successfully added _sync_checksum column")
        return True
    
    except Exception as e:
        conn.rollback()
        logger.error(f"Failed to add _sync_checksum column: {e}")
        raise
    finally:
        cursor.close()


def _execute_merge_operation(
    conn: pyodbc.Connection,
    schema_name: str,
    table_name: str,
    df: pd.DataFrame,
    merge_key_columns: Optional[List[str]],
    skip_on_error: bool
) -> Dict[str, Any]:
    """
    Execute MERGE operation to insert/update rows based on checksum comparison.
    
    Args:
        conn: pyodbc Connection
        schema_name: Schema name
        table_name: Table name
        df: DataFrame with data to merge (must include _sync_checksum column)
        merge_key_columns: Optional list of columns to use as merge keys
        skip_on_error: If True, skip rows with errors; if False, fail on first error
    
    Returns:
        Dictionary with merge statistics: inserted, updated, unchanged, skipped, error_details
    
    Raises:
        Exception: If merge operation fails and skip_on_error=False
    """
    cursor = conn.cursor()
    
    try:
        # Create temp table with same structure as destination
        temp_table_name = f"#temp_{table_name}_{int(time.time())}"
        
        logger.info(f"Creating temporary table {temp_table_name}")
        
        # Get column definitions from DataFrame
        columns_schema = _infer_mssql_schema_from_dataframe(df.drop(columns=['_sync_checksum']))
        
        # Build temp table create statement
        column_defs = []
        for col in columns_schema:
            column_defs.append(f"[{col['name']}] {col['type']}")
        column_defs.append("[_sync_checksum] VARCHAR(64)")
        
        create_temp_sql = f"""
            CREATE TABLE {temp_table_name} (
                {', '.join(column_defs)}
            )
        """
        cursor.execute(create_temp_sql)
        
        # Bulk insert data into temp table using fast_executemany (50-100x faster than row-by-row)
        logger.info(f"Inserting {len(df)} rows into temporary table using fast bulk insert")
        
        # Enable fast_executemany for pyodbc (critical for performance)
        cursor.fast_executemany = True
        
        # CRITICAL: Set input sizes for string columns to avoid "String data, right truncation" errors
        # When using fast_executemany, pyodbc uses default buffers (~510 bytes) regardless of column definition.
        # We must explicitly tell pyodbc to use larger buffers for string columns.
        input_sizes = []
        for col in df.columns:
            dtype_str = str(df[col].dtype)
            if dtype_str in ['object', 'string']:
                # Use SQL_WVARCHAR with (0, 0) for unlimited length - this is the fix for NVARCHAR(MAX)
                input_sizes.append((pyodbc.SQL_WVARCHAR, 0, 0))
            else:
                # Let pyodbc infer the type for non-string columns
                input_sizes.append(None)
        
        cursor.setinputsizes(input_sizes)
        logger.debug(f"Set input sizes for {len([s for s in input_sizes if s is not None])} string columns")
        
        # Build INSERT statement
        columns = ', '.join([f"[{col}]" for col in df.columns])
        placeholders = ', '.join(['?' for _ in df.columns])
        insert_sql = f"INSERT INTO {temp_table_name} ({columns}) VALUES ({placeholders})"
        
        # Prepare data as list of tuples, handling NaN/None values
        # Using itertuples is faster than iterrows
        def convert_value(val):
            """Convert pandas values to database-compatible format."""
            if pd.isna(val):
                return None
            # Handle numpy types
            if hasattr(val, 'item'):  # numpy scalar
                return val.item()
            return val
        
        # Convert DataFrame to list of tuples (much faster than row-by-row)
        data_tuples = []
        for row in df.itertuples(index=False, name=None):
            converted_row = tuple(convert_value(v) for v in row)
            data_tuples.append(converted_row)
        
        failed_rows = []
        successful_inserts = 0
        
        try:
            # Use executemany for bulk insert (with fast_executemany enabled, this is very fast)
            cursor.executemany(insert_sql, data_tuples)
            successful_inserts = len(data_tuples)
            logger.info(f"Successfully bulk inserted {successful_inserts} rows into temp table")
        
        except Exception as e:
            if skip_on_error:
                # Fall back to row-by-row insert to identify problematic rows
                logger.warning(f"Bulk insert failed: {e}. Falling back to row-by-row insert...")
                successful_inserts = 0
                
                for idx, row_tuple in enumerate(data_tuples):
                    try:
                        cursor.execute(insert_sql, row_tuple)
                        successful_inserts += 1
                    except Exception as row_error:
                        failed_rows.append({
                            'row_index': idx,
                            'error': str(row_error)
                        })
                        if len(failed_rows) <= 5:  # Log first 5 errors
                            logger.warning(f"Skipped row {idx}: {row_error}")
                
                logger.info(f"Row-by-row fallback: {successful_inserts} inserted, {len(failed_rows)} failed")
            else:
                logger.error(f"Bulk insert failed: {e}")
                raise
        
        # Build MERGE statement
        # Determine ON clause based on merge_key_columns
        if merge_key_columns:
            on_conditions = ' AND '.join([
                f"target.[{col}] = source.[{col}]" 
                for col in merge_key_columns
            ])
        else:
            # Use checksum for matching
            on_conditions = "target.[_sync_checksum] = source.[_sync_checksum]"
        
        # Get all data columns (excluding metadata columns)
        data_columns = [col for col in df.columns if col != '_sync_checksum']
        
        # Build UPDATE SET clause
        update_set = ', '.join([
            f"target.[{col}] = source.[{col}]" 
            for col in data_columns
        ])
        update_set += ", target.[_sync_checksum] = source.[_sync_checksum]"
        update_set += ", target.[_sync_updated_at] = GETDATE()"
        
        # Build INSERT columns and values
        insert_columns = ', '.join([f"[{col}]" for col in data_columns])
        insert_values = ', '.join([f"source.[{col}]" for col in data_columns])
        
        merge_sql = f"""
            MERGE [{schema_name}].[{table_name}] AS target
            USING {temp_table_name} AS source
            ON {on_conditions}
            WHEN MATCHED AND target.[_sync_checksum] <> source.[_sync_checksum] THEN
                UPDATE SET {update_set}
            WHEN NOT MATCHED BY TARGET THEN
                INSERT ({insert_columns}, [_sync_checksum], [_sync_updated_at])
                VALUES ({insert_values}, source.[_sync_checksum], GETDATE())
            OUTPUT $action;
        """
        
        logger.info(f"Executing MERGE operation")
        cursor.execute(merge_sql)
        
        # Count actions from OUTPUT
        actions = []
        for row in cursor.fetchall():
            actions.append(row[0])
        
        inserted = actions.count('INSERT')
        updated = actions.count('UPDATE')
        unchanged = len(df) - inserted - updated - len(failed_rows)
        
        conn.commit()
        
        logger.info(f"MERGE completed: {inserted} inserted, {updated} updated, {unchanged} unchanged, {len(failed_rows)} skipped")
        
        return {
            'inserted': inserted,
            'updated': updated,
            'unchanged': unchanged,
            'skipped': len(failed_rows),
            'error_details': failed_rows[:10] if failed_rows else []  # First 10 errors
        }
    
    except Exception as e:
        conn.rollback()
        logger.error(f"MERGE operation failed: {e}")
        if not skip_on_error:
            raise
        return {
            'inserted': 0,
            'updated': 0,
            'unchanged': 0,
            'skipped': len(df),
            'error_details': [{'error': str(e)}]
        }
    finally:
        # Clean up temp table
        try:
            cursor.execute(f"DROP TABLE IF EXISTS {temp_table_name}")
        except:
            pass
        cursor.close()


def sync_mssql_query_to_mssql(
    source_config: Dict[str, Any],
    source_query: str,
    dest_config: Dict[str, Any],
    dest_schema: str,
    dest_table: str,
    merge_key_columns: Optional[List[str]] = None,
    batch_size: int = 10000,
    validate_row_counts: bool = True,
    chunk_size: int = 1000,
    max_workers: int = 4,
    use_streaming: bool = True
) -> Dict[str, Any]:
    """
    Query source MSSQL database and sync results to destination MSSQL table.
    
    Features:
    - Streaming/chunked reading for large datasets (millions of records)
    - Concurrent chunk processing using thread pools
    - Checksum-based deduplication
    - Auto-create destination table
    - MERGE/UPSERT operations
    - Support for Entra ID and SQL Authentication
    - Configurable record-level error handling
    - Progress tracking for long-running operations
    
    Args:
        source_config: Source connection config (Entra ID or SQL Auth)
        source_query: SQL SELECT query to execute on source
        dest_config: Destination connection config (Entra ID or SQL Auth)
        dest_schema: Destination schema name (e.g., "dbo", "reporting")
        dest_table: Destination table name
        merge_key_columns: Optional columns to use as merge keys (default: use checksum)
        batch_size: Rows per batch for MERGE operation (default: 10000)
        validate_row_counts: Whether to validate row counts (default: True)
        chunk_size: Rows to read per chunk from source (default: 1000, optimized for streaming)
        max_workers: Number of concurrent workers for chunk processing (default: 4)
        use_streaming: If True, stream data in chunks; if False, load all at once (default: True)
    
    Returns:
        Dictionary with sync statistics and status
    
    Environment Variables:
        SKIP_ON_DATA_RECORD_LEVEL_ERROR: "true" or "false" (default: "false")
    
    Raises:
        ValueError: If inputs are invalid
        ConnectionError: If connection fails
        Exception: If sync fails and skip_on_error=False
    
    Performance Notes:
        - For datasets < 10K rows: use_streaming=False is faster
        - For datasets > 10K rows: use_streaming=True with chunk_size=1000
        - For datasets > 1M rows: use_streaming=True with max_workers=4-8
        - Concurrent processing is thread-safe and maintains data integrity
    """
    start_time = time.time()
    
    # Load environment configuration
    skip_on_error = os.getenv('SKIP_ON_DATA_RECORD_LEVEL_ERROR', 'false').lower() == 'true'
    logger.info(f"Record-level error handling: {'skip' if skip_on_error else 'fail-fast'}")
    
    # Validate inputs
    if not source_query or not source_query.strip():
        raise ValueError("source_query cannot be empty")
    
    # Basic query validation (prevent non-SELECT queries)
    query_upper = source_query.strip().upper()
    if not query_upper.startswith('SELECT'):
        raise ValueError("source_query must be a SELECT statement")
    
    if not dest_schema or not dest_table:
        raise ValueError("dest_schema and dest_table are required")
    
    source_conn = None
    dest_conn = None
    
    try:
        # Step 1: Connect to source MSSQL
        logger.info("=" * 60)
        logger.info("STEP 1: Connecting to source MSSQL database")
        logger.info("=" * 60)
        source_conn, source_auth_method = _get_mssql_connection(source_config)
        
        # Step 2: Determine processing strategy (streaming vs bulk)
        logger.info("=" * 60)
        logger.info(f"STEP 2: {'Streaming' if use_streaming else 'Bulk'} data processing")
        logger.info("=" * 60)
        
        if use_streaming:
            logger.info(f"Streaming enabled: chunk_size={chunk_size}, max_workers={max_workers}")
            logger.info(f"This optimizes memory usage and enables concurrent processing for large datasets")
        
        # Shared state for concurrent processing
        table_created = {'value': False, 'lock': Lock()}
        dest_auth_method = {'value': None, 'lock': Lock()}
        total_stats = {
            'rows_queried': 0,
            'rows_inserted': 0,
            'rows_updated': 0,
            'rows_unchanged': 0,
            'rows_skipped': 0,
            'checksum_errors': 0,
            'chunks_processed': 0,
            'lock': Lock()
        }
        
        def process_chunk(chunk_df: pd.DataFrame, chunk_num: int) -> Dict[str, int]:
            """
            Process a single chunk of data.
            
            Thread-Safety Pattern (Industry Standard):
            - pyodbc connections have threadsafety level 1 (module is thread-safe, 
              but connections/cursors cannot be shared across threads)
            - Each worker thread MUST have its own dedicated connection
            - This prevents "Connection is busy with results for another command" errors
            - Connections are opened at chunk start and closed in finally block
            
            References:
            - pyodbc wiki: "connections and cursors may not be used simultaneously by multiple threads"
            - ThreadPoolExecutor best practice: separate DB connections per worker thread
            """
            chunk_start = time.time()
            chunk_size_actual = len(chunk_df)
            
            logger.info(f"[Chunk {chunk_num}] Processing {chunk_size_actual} rows...")
            
            # Create thread-local destination connection
            chunk_dest_conn = None
            
            try:
                # CRITICAL: Each thread gets its own connection (pyodbc is NOT thread-safe)
                # Sharing a connection across threads causes: "Connection is busy with results for another command"
                chunk_dest_conn, chunk_auth_method = _get_mssql_connection(dest_config)
                
                # Store auth method once (thread-safe)
                with dest_auth_method['lock']:
                    if dest_auth_method['value'] is None:
                        dest_auth_method['value'] = chunk_auth_method
                
                # Generate checksums for this chunk using vectorized apply (faster than iterrows)
                checksum_columns = merge_key_columns if merge_key_columns else list(chunk_df.columns)
                chunk_checksum_errors = 0
                
                def generate_checksum_for_row(row):
                    """Generate checksum for a single row (used with apply)."""
                    nonlocal chunk_checksum_errors
                    checksum = _generate_row_checksum(row, checksum_columns, skip_on_error)
                    if checksum is None:
                        chunk_checksum_errors += 1
                    return checksum
                
                # Use apply() which is faster than iterrows() for this operation
                chunk_df = chunk_df.copy()  # Avoid SettingWithCopyWarning
                chunk_df['_sync_checksum'] = chunk_df.apply(generate_checksum_for_row, axis=1)
                
                # Filter out rows with failed checksums if skip_on_error
                if skip_on_error and chunk_checksum_errors > 0:
                    chunk_df = chunk_df[chunk_df['_sync_checksum'].notna()]
                    logger.warning(f"[Chunk {chunk_num}] Skipped {chunk_checksum_errors} rows with checksum errors")
                
                # Create/validate table (only once, thread-safe)
                with table_created['lock']:
                    if not table_created['value']:
                        logger.info(f"[Chunk {chunk_num}] Creating/validating destination table...")
                        
                        expected_schema = _infer_mssql_schema_from_dataframe(
                            chunk_df.drop(columns=['_sync_checksum'])
                        )
                        existing_schema = _get_table_schema(chunk_dest_conn, dest_schema, dest_table)
                        
                        if existing_schema is None:
                            _create_destination_table(chunk_dest_conn, dest_schema, dest_table, expected_schema)
                            logger.info(f"[Chunk {chunk_num}] ✓ Table created successfully")
                        else:
                            logger.info(f"[Chunk {chunk_num}] Table exists, validating schema...")
                            validation_result = _validate_table_schema(existing_schema, expected_schema)
                            
                            if not validation_result['is_valid']:
                                if not validation_result['has_checksum_column']:
                                    _add_checksum_column_to_table(chunk_dest_conn, dest_schema, dest_table)
                                
                                if validation_result['missing_columns'] or validation_result['type_mismatches']:
                                    error_msg = "Schema incompatibility detected:\n"
                                    for issue in validation_result['issues']:
                                        error_msg += f"  - {issue}\n"
                                    raise ValueError(error_msg)
                        
                        table_created['value'] = True
                
                # Execute MERGE operation for this chunk (using thread-local connection)
                merge_result = _execute_merge_operation(
                    chunk_dest_conn,
                    dest_schema,
                    dest_table,
                    chunk_df,
                    merge_key_columns,
                    skip_on_error
                )
                
                chunk_time = time.time() - chunk_start
                logger.info(
                    f"[Chunk {chunk_num}] ✓ Completed in {chunk_time:.2f}s: "
                    f"{merge_result['inserted']} inserted, {merge_result['updated']} updated, "
                    f"{merge_result['unchanged']} unchanged, {merge_result['skipped']} skipped"
                )
                
                return {
                    'rows_queried': chunk_size_actual,
                    'rows_inserted': merge_result['inserted'],
                    'rows_updated': merge_result['updated'],
                    'rows_unchanged': merge_result['unchanged'],
                    'rows_skipped': merge_result['skipped'],
                    'checksum_errors': chunk_checksum_errors
                }
            
            except Exception as e:
                logger.error(f"[Chunk {chunk_num}] Failed: {e}")
                if not skip_on_error:
                    raise
                return {
                    'rows_queried': chunk_size_actual,
                    'rows_inserted': 0,
                    'rows_updated': 0,
                    'rows_unchanged': 0,
                    'rows_skipped': chunk_size_actual,
                    'checksum_errors': 0
                }
            
            finally:
                # Always close thread-local connection
                if chunk_dest_conn:
                    try:
                        chunk_dest_conn.close()
                    except:
                        pass
        
        # Step 4: Execute query and process data
        logger.info("=" * 60)
        logger.info("STEP 4: Executing source query and processing data")
        logger.info("=" * 60)
        logger.info(f"Query: {source_query[:200]}...")
        
        if use_streaming:
            # Streaming mode: read and process chunks concurrently
            chunk_iterator = pd.read_sql(source_query, source_conn, chunksize=chunk_size)
            
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = []
                chunk_num = 0
                
                for chunk_df in chunk_iterator:
                    chunk_num += 1
                    future = executor.submit(process_chunk, chunk_df, chunk_num)
                    futures.append(future)
                
                # Wait for all chunks to complete and aggregate results
                for future in as_completed(futures):
                    chunk_result = future.result()
                    with total_stats['lock']:
                        total_stats['rows_queried'] += chunk_result['rows_queried']
                        total_stats['rows_inserted'] += chunk_result['rows_inserted']
                        total_stats['rows_updated'] += chunk_result['rows_updated']
                        total_stats['rows_unchanged'] += chunk_result['rows_unchanged']
                        total_stats['rows_skipped'] += chunk_result['rows_skipped']
                        total_stats['checksum_errors'] += chunk_result['checksum_errors']
                        total_stats['chunks_processed'] += 1
                
                logger.info(f"✓ Processed {total_stats['chunks_processed']} chunks concurrently")
        
        else:
            # Bulk mode: read all data at once (original behavior for small datasets)
            df = pd.read_sql(source_query, source_conn)
            rows_queried = len(df)
            logger.info(f"Query returned {rows_queried} rows with {len(df.columns)} columns")
            
            if rows_queried == 0:
                logger.warning("Query returned 0 rows. Nothing to sync.")
                # Need to get auth method for empty result
                temp_conn, temp_auth = _get_mssql_connection(dest_config)
                temp_conn.close()
                
                return {
                    'status': 'success',
                    'source_query': source_query,
                    'destination': f"{dest_schema}.{dest_table}",
                    'authentication_method': f"source:{source_auth_method}, dest:{temp_auth}",
                    'result': {
                        'rows_queried': 0,
                        'rows_inserted': 0,
                        'rows_updated': 0,
                        'rows_unchanged': 0,
                        'rows_skipped': 0,
                        'processing_time_seconds': time.time() - start_time,
                        'checksum_column': '_sync_checksum',
                        'skip_on_error_enabled': skip_on_error,
                        'streaming_enabled': False
                    }
                }
            
            # Process as single chunk
            chunk_result = process_chunk(df, 1)
            total_stats['rows_queried'] = chunk_result['rows_queried']
            total_stats['rows_inserted'] = chunk_result['rows_inserted']
            total_stats['rows_updated'] = chunk_result['rows_updated']
            total_stats['rows_unchanged'] = chunk_result['rows_unchanged']
            total_stats['rows_skipped'] = chunk_result['rows_skipped']
            total_stats['checksum_errors'] = chunk_result['checksum_errors']
            total_stats['chunks_processed'] = 1
        
        # Step 5: Finalize and report results
        logger.info("=" * 60)
        logger.info("STEP 5: Finalizing sync operation")
        logger.info("=" * 60)
        
        processing_time = time.time() - start_time
        
        # Determine status
        if total_stats['rows_skipped'] > 0:
            status = 'partial_success'
        else:
            status = 'success'
        
        # Build result
        result = {
            'status': status,
            'source_query': source_query,
            'destination': f"{dest_schema}.{dest_table}",
            'authentication_method': f"source:{source_auth_method}, dest:{dest_auth_method['value']}",
            'result': {
                'rows_queried': total_stats['rows_queried'],
                'rows_with_checksum_errors': total_stats['checksum_errors'],
                'rows_inserted': total_stats['rows_inserted'],
                'rows_updated': total_stats['rows_updated'],
                'rows_unchanged': total_stats['rows_unchanged'],
                'rows_skipped': total_stats['rows_skipped'],
                'chunks_processed': total_stats['chunks_processed'],
                'processing_time_seconds': round(processing_time, 2),
                'checksum_column': '_sync_checksum',
                'skip_on_error_enabled': skip_on_error,
                'streaming_enabled': use_streaming,
                'chunk_size': chunk_size if use_streaming else None,
                'max_workers': max_workers if use_streaming else None,
                'error_summary': {
                    'checksum_errors': total_stats['checksum_errors'],
                    'merge_errors': total_stats['rows_skipped'],
                    'first_10_errors': []  # Aggregated from all chunks
                }
            }
        }
        
        logger.info("=" * 60)
        logger.info(f"SYNC COMPLETED: {status.upper()}")
        logger.info(f"Total rows processed: {total_stats['rows_queried']}")
        logger.info(f"Results: {total_stats['rows_inserted']} inserted, {total_stats['rows_updated']} updated, "
                   f"{total_stats['rows_unchanged']} unchanged, {total_stats['rows_skipped']} skipped")
        if use_streaming:
            logger.info(f"Chunks processed: {total_stats['chunks_processed']} (concurrent with {max_workers} workers)")
        logger.info(f"Processing time: {processing_time:.2f} seconds")
        logger.info("=" * 60)
        
        return result
    
    except Exception as e:
        logger.error(f"Sync failed: {e}")
        return {
            'status': 'error',
            'source_query': source_query,
            'destination': f"{dest_schema}.{dest_table}",
            'error': str(e),
            'result': {
                'processing_time_seconds': round(time.time() - start_time, 2)
            }
        }
    
    finally:
        # Cleanup source connection (destination connections are closed by each thread)
        if source_conn:
            try:
                source_conn.close()
                logger.info("Source connection closed")
            except:
                pass
