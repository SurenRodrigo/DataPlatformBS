"""
Unit tests for MSSQL to MSSQL Sync Utility

Tests cover:
- Checksum generation
- Connection string building (Entra ID and SQL Auth)
- Schema inference
- Type mapping
- Error handling (skip vs fail-fast modes)
"""

import unittest
import pandas as pd
import numpy as np
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime
import os
import sys

# Add parent directory to path
sys.path.insert(0, '/app/data-manager')

from pyairbyte.utils.mssql_to_mssql_sync import (
    _generate_row_checksum,
    _pandas_dtype_to_mssql,
    _infer_mssql_schema_from_dataframe,
    _validate_table_schema,
    _are_types_compatible,
    PANDAS_TO_MSSQL_TYPE_MAP
)


class TestChecksumGeneration(unittest.TestCase):
    """Test checksum generation functionality."""
    
    def test_checksum_consistency(self):
        """Test that same data produces same checksum."""
        row = pd.Series({
            'id': 1,
            'name': 'John Doe',
            'amount': 100.50,
            'date': pd.Timestamp('2024-01-01')
        })
        
        columns = ['id', 'name', 'amount', 'date']
        
        checksum1 = _generate_row_checksum(row, columns, skip_on_error=False)
        checksum2 = _generate_row_checksum(row, columns, skip_on_error=False)
        
        self.assertEqual(checksum1, checksum2)
        self.assertEqual(len(checksum1), 64)  # SHA256 hex digest length
    
    def test_checksum_null_handling(self):
        """Test that NULL values are handled consistently."""
        row = pd.Series({
            'id': 1,
            'name': None,
            'amount': np.nan,
            'date': pd.NaT
        })
        
        columns = ['id', 'name', 'amount', 'date']
        
        checksum = _generate_row_checksum(row, columns, skip_on_error=False)
        
        self.assertIsNotNone(checksum)
        self.assertEqual(len(checksum), 64)
    
    def test_checksum_different_data(self):
        """Test that different data produces different checksums."""
        row1 = pd.Series({'id': 1, 'name': 'Alice'})
        row2 = pd.Series({'id': 1, 'name': 'Bob'})
        
        columns = ['id', 'name']
        
        checksum1 = _generate_row_checksum(row1, columns, skip_on_error=False)
        checksum2 = _generate_row_checksum(row2, columns, skip_on_error=False)
        
        self.assertNotEqual(checksum1, checksum2)
    
    def test_checksum_skip_on_error_true(self):
        """Test skip_on_error=True returns None on error."""
        row = pd.Series({'id': 1, 'name': 'John'})
        columns = ['invalid_column']  # Column doesn't exist
        
        result = _generate_row_checksum(row, columns, skip_on_error=True)
        
        self.assertIsNone(result)
    
    def test_checksum_skip_on_error_false(self):
        """Test skip_on_error=False raises exception on error."""
        row = pd.Series({'id': 1, 'name': 'John'})
        columns = ['invalid_column']  # Column doesn't exist
        
        with self.assertRaises(ValueError):
            _generate_row_checksum(row, columns, skip_on_error=False)
    
    def test_checksum_data_types(self):
        """Test checksum handles various data types."""
        row = pd.Series({
            'int_col': 42,
            'float_col': 3.14159,
            'bool_col': True,
            'str_col': 'test',
            'datetime_col': pd.Timestamp('2024-01-15 10:30:00'),
            'null_col': None
        })
        
        columns = list(row.index)
        
        checksum = _generate_row_checksum(row, columns, skip_on_error=False)
        
        self.assertIsNotNone(checksum)
        self.assertEqual(len(checksum), 64)


class TestSchemaInference(unittest.TestCase):
    """Test schema inference from DataFrame."""
    
    def test_infer_basic_types(self):
        """Test inference of basic data types."""
        df = pd.DataFrame({
            'int_col': [1, 2, 3],
            'float_col': [1.1, 2.2, 3.3],
            'str_col': ['a', 'b', 'c'],
            'bool_col': [True, False, True],
            'datetime_col': pd.to_datetime(['2024-01-01', '2024-01-02', '2024-01-03'])
        })
        
        schema = _infer_mssql_schema_from_dataframe(df)
        
        self.assertEqual(len(schema), 5)
        self.assertEqual(schema[0]['name'], 'int_col')
        self.assertIn('BIGINT', schema[0]['type'])
        self.assertEqual(schema[1]['name'], 'float_col')
        self.assertIn('FLOAT', schema[1]['type'])
        self.assertEqual(schema[4]['name'], 'datetime_col')
        self.assertIn('DATETIME2', schema[4]['type'])
    
    def test_infer_nullable_columns(self):
        """Test detection of nullable columns.
        
        NEW BEHAVIOR: All columns are nullable by default (force_nullable=True)
        This prevents NULL constraint violations when later chunks have NULLs
        that weren't present in the first chunk.
        """
        df = pd.DataFrame({
            'col_with_nulls': [1, None, 3],
            'col_no_nulls': [1, 2, 3]
        })
        
        # Default behavior: force_nullable=True (all columns nullable)
        schema = _infer_mssql_schema_from_dataframe(df)
        nullable_col = next(s for s in schema if s['name'] == 'col_with_nulls')
        non_nullable_col = next(s for s in schema if s['name'] == 'col_no_nulls')
        
        # NEW: Both columns should be nullable for streaming safety
        self.assertTrue(nullable_col['nullable'])
        self.assertTrue(non_nullable_col['nullable'])  # Changed: was False, now True for safety
        
        # Test legacy behavior with force_nullable=False
        schema_legacy = _infer_mssql_schema_from_dataframe(df, force_nullable=False)
        nullable_col_legacy = next(s for s in schema_legacy if s['name'] == 'col_with_nulls')
        non_nullable_col_legacy = next(s for s in schema_legacy if s['name'] == 'col_no_nulls')
        
        self.assertTrue(nullable_col_legacy['nullable'])
        self.assertFalse(non_nullable_col_legacy['nullable'])
    
    def test_infer_string_lengths(self):
        """Test string length inference."""
        df = pd.DataFrame({
            'short_str': ['abc', 'def', 'ghi'],
            'long_str': ['a' * 100, 'b' * 150, 'c' * 200],
            'very_long_str': ['x' * 5000, 'y' * 6000, 'z' * 7000]
        })
        
        schema = _infer_mssql_schema_from_dataframe(df)
        
        short_str_schema = next(s for s in schema if s['name'] == 'short_str')
        long_str_schema = next(s for s in schema if s['name'] == 'long_str')
        very_long_str_schema = next(s for s in schema if s['name'] == 'very_long_str')
        
        # NEW BEHAVIOR: All strings use NVARCHAR(MAX) for streaming safety
        # This prevents truncation errors when later chunks have longer strings
        self.assertEqual(short_str_schema['type'], 'NVARCHAR(MAX)')
        self.assertEqual(long_str_schema['type'], 'NVARCHAR(MAX)')
        self.assertEqual(very_long_str_schema['type'], 'NVARCHAR(MAX)')


class TestTypeMapping(unittest.TestCase):
    """Test type mapping."""
    
    def test_pandas_to_mssql_type_mapping(self):
        """Test pandas to MSSQL type mapping with generous/safe types."""
        # NEW BEHAVIOR: All integer types map to BIGINT for safety
        # This prevents overflow when later chunks have larger values
        test_cases = [
            ('int64', 'BIGINT'),
            ('int32', 'BIGINT'),  # Changed: was INT, now BIGINT for safety
            ('float64', 'FLOAT'),
            ('bool', 'BIT'),
            ('datetime64[ns]', 'DATETIME2'),
        ]
        
        for pandas_type, expected_mssql_type in test_cases:
            series = pd.Series([1, 2, 3], dtype=pandas_type if pandas_type != 'datetime64[ns]' else 'datetime64[ns]')
            result = _pandas_dtype_to_mssql(pandas_type, series)
            self.assertIn(expected_mssql_type, result, f"Failed for {pandas_type}")
    
    def test_unknown_dtype_fallback(self):
        """Test that unknown dtypes fall back to NVARCHAR(MAX)."""
        series = pd.Series([1, 2, 3])
        result = _pandas_dtype_to_mssql('unknown_type', series)
        self.assertEqual(result, 'NVARCHAR(MAX)')


class TestConnectionStringBuilding(unittest.TestCase):
    """Test connection string patterns (mock-based)."""
    
    @patch('pyairbyte.utils.mssql_to_mssql_sync.pyodbc.drivers')
    @patch('pyairbyte.utils.mssql_to_mssql_sync.pyodbc.connect')
    def test_entra_id_connection_config(self, mock_connect, mock_drivers):
        """Test Entra ID Service Principal connection configuration."""
        from pyairbyte.utils.mssql_to_mssql_sync import _get_mssql_connection
        
        # Mock available drivers
        mock_drivers.return_value = ['ODBC Driver 18 for SQL Server']
        
        # Mock successful connection
        mock_conn = Mock()
        mock_connect.return_value = mock_conn
        
        config = {
            'server': 'test.database.windows.net',
            'database': 'testdb',
            'client_id': 'test-client-id',
            'client_secret': 'test-secret',
            'tenant_id': 'test-tenant-id',
            'port': '1433'
        }
        
        conn, auth_method = _get_mssql_connection(config, max_retries=1)
        
        # Verify connection was called
        mock_connect.assert_called_once()
        call_args = mock_connect.call_args[0][0]
        
        # Verify Entra ID specific settings
        self.assertIn('Authentication=ActiveDirectoryServicePrincipal', call_args)
        self.assertIn('Encrypt=yes', call_args)
        self.assertIn('TrustServerCertificate=no', call_args)
        self.assertIn(config['client_id'], call_args)
        self.assertEqual(auth_method, 'service_principal')
    
    @patch('pyairbyte.utils.mssql_to_mssql_sync.pyodbc.drivers')
    @patch('pyairbyte.utils.mssql_to_mssql_sync.pyodbc.connect')
    def test_sql_auth_connection_config(self, mock_connect, mock_drivers):
        """Test SQL Authentication connection configuration."""
        from pyairbyte.utils.mssql_to_mssql_sync import _get_mssql_connection
        
        # Mock available drivers
        mock_drivers.return_value = ['ODBC Driver 18 for SQL Server']
        
        # Mock successful connection
        mock_conn = Mock()
        mock_connect.return_value = mock_conn
        
        config = {
            'server': 'localhost',
            'database': 'testdb',
            'username': 'testuser',
            'password': 'testpass'
        }
        
        conn, auth_method = _get_mssql_connection(config, max_retries=1)
        
        # Verify connection was called
        mock_connect.assert_called_once()
        call_args = mock_connect.call_args[0][0]
        
        # Verify SQL Auth specific settings
        self.assertIn('UID=testuser', call_args)
        self.assertIn('PWD=testpass', call_args)
        self.assertIn('TrustServerCertificate=yes', call_args)
        self.assertNotIn('Authentication=ActiveDirectoryServicePrincipal', call_args)
        self.assertEqual(auth_method, 'sql_auth')
    
    @patch('pyairbyte.utils.mssql_to_mssql_sync.pyodbc.drivers')
    def test_missing_credentials(self, mock_drivers):
        """Test that missing credentials raise ValueError."""
        from pyairbyte.utils.mssql_to_mssql_sync import _get_mssql_connection
        
        mock_drivers.return_value = ['ODBC Driver 18 for SQL Server']
        
        config = {
            'server': 'localhost',
            'database': 'testdb'
            # Missing both Entra ID and SQL Auth credentials
        }
        
        with self.assertRaises(ValueError) as context:
            _get_mssql_connection(config, max_retries=1)
        
        self.assertIn('credentials', str(context.exception).lower())


class TestErrorHandlingModes(unittest.TestCase):
    """Test skip_on_error vs fail-fast modes."""
    
    def test_skip_on_error_environment_variable(self):
        """Test that environment variable controls error handling mode."""
        row = pd.Series({'id': 1})
        
        # Test with SKIP_ON_DATA_RECORD_LEVEL_ERROR=false
        os.environ['SKIP_ON_DATA_RECORD_LEVEL_ERROR'] = 'false'
        
        # Should raise when skip_on_error=False
        with self.assertRaises(ValueError):
            _generate_row_checksum(row, ['nonexistent'], skip_on_error=False)
        
        # Test with SKIP_ON_DATA_RECORD_LEVEL_ERROR=true
        os.environ['SKIP_ON_DATA_RECORD_LEVEL_ERROR'] = 'true'
        
        # Should return None when skip_on_error=True
        result = _generate_row_checksum(row, ['nonexistent'], skip_on_error=True)
        self.assertIsNone(result)
        
        # Clean up
        del os.environ['SKIP_ON_DATA_RECORD_LEVEL_ERROR']


class TestMergeKeyColumns(unittest.TestCase):
    """Test merge key column handling."""
    
    def test_merge_key_columns_vs_full_checksum(self):
        """Test difference between merge keys and full row checksum."""
        row1 = pd.Series({'id': 1, 'name': 'Alice', 'age': 30})
        row2 = pd.Series({'id': 1, 'name': 'Alice', 'age': 31})  # Age changed
        
        # Using only 'id' and 'name' as merge keys
        checksum1_partial = _generate_row_checksum(row1, ['id', 'name'], skip_on_error=False)
        checksum2_partial = _generate_row_checksum(row2, ['id', 'name'], skip_on_error=False)
        
        # Using all columns
        checksum1_full = _generate_row_checksum(row1, ['id', 'name', 'age'], skip_on_error=False)
        checksum2_full = _generate_row_checksum(row2, ['id', 'name', 'age'], skip_on_error=False)
        
        # Partial checksums should match (id and name are same)
        self.assertEqual(checksum1_partial, checksum2_partial)
        
        # Full checksums should differ (age changed)
        self.assertNotEqual(checksum1_full, checksum2_full)


class TestSchemaValidation(unittest.TestCase):
    """Test schema validation functionality."""
    
    def test_validate_identical_schemas(self):
        """Test validation with identical schemas."""
        existing_schema = [
            {'name': 'id', 'type': 'INT', 'nullable': False, 'max_length': None},
            {'name': 'name', 'type': 'NVARCHAR(100)', 'nullable': True, 'max_length': 100},
            {'name': '_sync_checksum', 'type': 'VARCHAR(64)', 'nullable': True, 'max_length': 64}
        ]
        
        expected_schema = [
            {'name': 'id', 'type': 'INT', 'nullable': False},
            {'name': 'name', 'type': 'NVARCHAR(100)', 'nullable': True}
        ]
        
        result = _validate_table_schema(existing_schema, expected_schema)
        
        self.assertTrue(result['is_valid'])
        self.assertEqual(len(result['missing_columns']), 0)
        self.assertEqual(len(result['type_mismatches']), 0)
        self.assertTrue(result['has_checksum_column'])
    
    def test_validate_missing_columns(self):
        """Test detection of missing columns in destination."""
        existing_schema = [
            {'name': 'id', 'type': 'INT', 'nullable': False, 'max_length': None},
            {'name': '_sync_checksum', 'type': 'VARCHAR(64)', 'nullable': True, 'max_length': 64}
        ]
        
        expected_schema = [
            {'name': 'id', 'type': 'INT', 'nullable': False},
            {'name': 'name', 'type': 'NVARCHAR(100)', 'nullable': True},
            {'name': 'email', 'type': 'NVARCHAR(255)', 'nullable': True}
        ]
        
        result = _validate_table_schema(existing_schema, expected_schema)
        
        self.assertFalse(result['is_valid'])
        self.assertIn('name', result['missing_columns'])
        self.assertIn('email', result['missing_columns'])
    
    def test_validate_type_mismatches(self):
        """Test detection of type mismatches."""
        existing_schema = [
            {'name': 'id', 'type': 'NVARCHAR(50)', 'nullable': False, 'max_length': 50},
            {'name': 'amount', 'type': 'INT', 'nullable': True, 'max_length': None},
            {'name': '_sync_checksum', 'type': 'VARCHAR(64)', 'nullable': True, 'max_length': 64}
        ]
        
        expected_schema = [
            {'name': 'id', 'type': 'INT', 'nullable': False},  # Type mismatch: string vs int
            {'name': 'amount', 'type': 'NVARCHAR(100)', 'nullable': True}  # Type mismatch: int vs string
        ]
        
        result = _validate_table_schema(existing_schema, expected_schema)
        
        self.assertFalse(result['is_valid'])
        self.assertEqual(len(result['type_mismatches']), 2)
    
    def test_validate_missing_checksum_column(self):
        """Test detection of missing _sync_checksum column."""
        existing_schema = [
            {'name': 'id', 'type': 'INT', 'nullable': False, 'max_length': None},
            {'name': 'name', 'type': 'NVARCHAR(100)', 'nullable': True, 'max_length': 100}
        ]
        
        expected_schema = [
            {'name': 'id', 'type': 'INT', 'nullable': False},
            {'name': 'name', 'type': 'NVARCHAR(100)', 'nullable': True}
        ]
        
        result = _validate_table_schema(existing_schema, expected_schema)
        
        self.assertFalse(result['is_valid'])
        self.assertFalse(result['has_checksum_column'])
        self.assertTrue(any('_sync_checksum' in issue for issue in result['issues']))
    
    def test_validate_extra_columns_ok(self):
        """Test that extra columns in destination are OK."""
        existing_schema = [
            {'name': 'id', 'type': 'INT', 'nullable': False, 'max_length': None},
            {'name': 'name', 'type': 'NVARCHAR(100)', 'nullable': True, 'max_length': 100},
            {'name': 'extra_col', 'type': 'VARCHAR(50)', 'nullable': True, 'max_length': 50},
            {'name': '_sync_checksum', 'type': 'VARCHAR(64)', 'nullable': True, 'max_length': 64}
        ]
        
        expected_schema = [
            {'name': 'id', 'type': 'INT', 'nullable': False},
            {'name': 'name', 'type': 'NVARCHAR(100)', 'nullable': True}
        ]
        
        result = _validate_table_schema(existing_schema, expected_schema)
        
        self.assertTrue(result['is_valid'])  # Extra columns don't make it invalid
        self.assertIn('extra_col', result['extra_columns'])


class TestTypeCompatibility(unittest.TestCase):
    """Test type compatibility checks."""
    
    def test_exact_match(self):
        """Test exact type match."""
        self.assertTrue(_are_types_compatible('INT', 'INT'))
        self.assertTrue(_are_types_compatible('NVARCHAR(100)', 'NVARCHAR(100)'))
    
    def test_numeric_compatibility(self):
        """Test numeric type compatibility."""
        self.assertTrue(_are_types_compatible('BIGINT', 'INT'))
        self.assertTrue(_are_types_compatible('INT', 'SMALLINT'))
        self.assertTrue(_are_types_compatible('FLOAT', 'REAL'))
    
    def test_string_length_compatibility(self):
        """Test string length compatibility."""
        # Existing larger or equal is OK
        self.assertTrue(_are_types_compatible('NVARCHAR(200)', 'NVARCHAR(100)'))
        # Existing smaller is NOT OK
        self.assertFalse(_are_types_compatible('NVARCHAR(50)', 'NVARCHAR(100)'))
        # MAX is always compatible
        self.assertTrue(_are_types_compatible('NVARCHAR(MAX)', 'NVARCHAR(1000)'))
        self.assertTrue(_are_types_compatible('NVARCHAR(1000)', 'NVARCHAR(MAX)'))
    
    def test_datetime_compatibility(self):
        """Test datetime type compatibility."""
        self.assertTrue(_are_types_compatible('DATETIME2', 'DATETIME'))
        self.assertTrue(_are_types_compatible('DATETIME', 'SMALLDATETIME'))
    
    def test_incompatible_types(self):
        """Test incompatible types."""
        self.assertFalse(_are_types_compatible('INT', 'NVARCHAR(50)'))
        self.assertFalse(_are_types_compatible('DATETIME', 'INT'))
        self.assertFalse(_are_types_compatible('BIT', 'FLOAT'))


def run_tests():
    """Run all tests."""
    unittest.main(argv=[''], verbosity=2, exit=False)


if __name__ == '__main__':
    run_tests()
