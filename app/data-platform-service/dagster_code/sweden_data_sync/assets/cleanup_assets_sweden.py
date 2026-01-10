import sys
import os
from dagster import asset, MetadataValue, AssetExecutionContext

# Add the data-manager path to sys.path for imports
sys.path.append('/app/data-manager')
sys.path.append('/app')

from pyairbyte.utils.cache_db_manager import PyAirbyteCacheDBManager
from pyairbyte.utils.common_cache import CACHE_CONFIGS
import psycopg2

def _drop_associated_types_sweden(context: AssetExecutionContext, cache_config: dict):
    """
    Drop associated types in the Sweden cache schema to prevent conflicts.
    This handles the case where PyAirbyte creates custom types that can cause
    unique constraint violations when trying to recreate tables.
    """
    host = cache_config['host']
    dbname = cache_config['database']
    user = cache_config['username']
    password = cache_config['password']
    port = str(cache_config['port'])
    schema_name = cache_config['schema_name']
    
    conn = None
    try:
        conn = psycopg2.connect(
            host=host, dbname=dbname, user=user, password=password, port=port
        )
        conn.autocommit = True
        with conn.cursor() as cur:
            # Drop types that might cause conflicts
            types_to_drop = ['_airbyte_streams', '__airbyte_streams']
            for type_name in types_to_drop:
                try:
                    context.log.info(f"Dropping type: {schema_name}.{type_name}")
                    cur.execute(f'DROP TYPE IF EXISTS {schema_name}.{type_name} CASCADE;')
                    context.log.info(f"✓ Successfully dropped type {type_name}")
                except Exception as e:
                    context.log.warning(f"✗ Failed to drop type {type_name}: {e}")
        
        context.log.info("Associated types cleanup completed")
    except Exception as e:
        context.log.error(f"Error dropping associated types: {e}")
    finally:
        if conn:
            conn.close()

@asset(
    name="cleanup_airbyte_cache_tables_sweden",
    group_name="sweden_data_sync"
)
def cleanup_airbyte_cache_tables_sweden(context: AssetExecutionContext):
    """
    Dagster asset to cleanup unnecessary airbyte cache tables for Sweden cache before data sync.
    This uses the Sweden cache configuration from common_cache.py.
    """
    
    context.log.info("Starting cleanup of Sweden airbyte cache tables...")
    
    try:
        # Get Sweden cache configuration from common_cache
        sweden_cache_config = CACHE_CONFIGS["sweden"]
        context.log.info(f"Using Sweden cache configuration: schema={sweden_cache_config['schema_name']}")
        
        # Create PyAirbyteCacheDBManager with Sweden cache configuration
        db_manager = PyAirbyteCacheDBManager(cache_config=sweden_cache_config)
        
        tables_to_drop = [
            '_airbyte_destination_state',
            '_airbyte_state', 
            '_airbyte_streams',
            'sync_metadata'
        ]
        
        dropped_tables = []
        failed_tables = []
        
        for table in tables_to_drop:
            context.log.info(f"Dropping table: {table}")
            success = db_manager.drop_cache_table(table)
            
            if success:
                dropped_tables.append(table)
                context.log.info(f"✓ Successfully dropped {table}")
            else:
                failed_tables.append(table)
                context.log.warning(f"✗ Failed to drop {table}")
        
        # Also drop associated types that might cause conflicts
        context.log.info("Dropping associated types to prevent conflicts...")
        _drop_associated_types_sweden(context, sweden_cache_config)
        
        context.log.info(f"Sweden cache cleanup completed. Dropped {len(dropped_tables)} tables.")
        
        # Return cleanup summary
        cleanup_result = {
            "status": "success",
            "dropped_tables": dropped_tables,
            "failed_tables": failed_tables,
            "total_dropped": len(dropped_tables),
            "total_failed": len(failed_tables),
            "cache_schema": sweden_cache_config['schema_name']
        }
        
        context.add_output_metadata({
            "status": MetadataValue.text("success"),
            "tables_dropped": MetadataValue.text(", ".join(dropped_tables)),
            "tables_failed": MetadataValue.text(", ".join(failed_tables)),
            "total_dropped": MetadataValue.int(len(dropped_tables)),
            "total_failed": MetadataValue.int(len(failed_tables)),
            "cache_schema": MetadataValue.text(sweden_cache_config['schema_name'])
        })
        
        return cleanup_result
        
    except Exception as e:
        context.log.error(f"Error during Sweden cache cleanup: {str(e)}")
        
        cleanup_result = {
            "status": "error",
            "error": str(e),
            "dropped_tables": [],
            "failed_tables": [],
            "total_dropped": 0,
            "total_failed": 0,
            "cache_schema": "sweden_cache_error"
        }
        
        context.add_output_metadata({
            "status": MetadataValue.text("error"),
            "error": MetadataValue.text(str(e))
        })
        
        # Don't fail the pipeline, just return error status
        return cleanup_result


# =============================================================================
# TARGETED CLEANUP ASSETS FOR SWEDEN CACHE (PER CONNECTOR)
# =============================================================================
# This section provides targeted cleanup assets for specific connectors using Sweden cache:
# 1. cleanup_ditio_case_airbyte_cache_sweden - Truncates Ditio NRCG User API cache tables in Sweden cache
# 2. cleanup_tqm_case_airbyte_cache_sweden - Truncates TQM API cache tables in Sweden cache
# These assets are used by specific jobs to clean only relevant cache tables
# before syncing data, improving efficiency and avoiding unnecessary cleanup.
# =============================================================================

def _truncate_tables_sweden(context: AssetExecutionContext, tables: list[str]):
    """
    Truncate only the specified cache tables using Sweden cache configuration.
    """
    # Get Sweden cache configuration
    sweden_cache_config = CACHE_CONFIGS["sweden"]
    
    host = sweden_cache_config['host']
    dbname = sweden_cache_config['database']
    user = sweden_cache_config['username']
    password = sweden_cache_config['password']
    port = sweden_cache_config['port']
    schema_name = sweden_cache_config['schema_name']

    if not tables:
        context.log.info("No tables specified for Sweden targeted cleanup; skipping.")
        return {"status": "skipped", "tables": [], "cache_schema": schema_name}

    conn = None
    try:
        conn = psycopg2.connect(
            host=host,
            dbname=dbname,
            user=user,
            password=password,
            port=port,
        )
        conn.autocommit = True
        with conn.cursor() as cur:
            for table in tables:
                # Use schema-qualified table names
                full_table_name = f"{schema_name}.{table}"
                context.log.info(f"Truncating Sweden cache table: {full_table_name}")
                cur.execute(f'TRUNCATE TABLE {full_table_name} RESTART IDENTITY CASCADE;')
        context.log.info(f"Sweden targeted cleanup completed. Truncated {len(tables)} tables.")
        return {"status": "success", "tables": tables, "cache_schema": schema_name}
    except Exception as e:
        context.log.error(f"Sweden targeted cleanup failed: {e}")
        return {"status": "error", "error": str(e), "tables": tables, "cache_schema": schema_name}
    finally:
        if conn:
            conn.close()


@asset(name="cleanup_ditio_case_airbyte_cache_sweden", group_name="create_tqm_cases_job")
def cleanup_ditio_case_airbyte_cache_sweden(context: AssetExecutionContext):
    """
    Truncates Ditio NRCG User API cache tables for the create_tqm_cases_job flow using Sweden cache.
    
    This asset:
    1. Truncates source_ditio_incidents_registration table in Sweden cache
    2. Is used by dto_nrcg_sync_cases_asset as a dependency
    3. Ensures clean Sweden cache before syncing incidents_registration stream
    4. Improves efficiency by targeting only relevant tables
    """
    tables = [
        "source_ditio_incidents_registration"
    ]
    return _truncate_tables_sweden(context, tables)


@asset(name="cleanup_tqm_case_airbyte_cache_sweden", group_name="patch_ditio_cases_job")
def cleanup_tqm_case_airbyte_cache_sweden(context: AssetExecutionContext):
    """
    Truncates TQM API cache tables for the patch_ditio_cases_job flow using Sweden cache.
    
    This asset:
    1. Truncates source_tqm_case table in Sweden cache
    2. Is used by tqm_api_sync_cases_asset as a dependency
    3. Ensures clean Sweden cache before syncing case stream
    4. Improves efficiency by targeting only relevant tables
    """
    tables = [
        "source_tqm_case"
    ]
    return _truncate_tables_sweden(context, tables)


@asset(
    name="cleanup_sweden_cache_using_manager",
    group_name="nrc_data_sync"
)
def cleanup_sweden_cache_using_manager(context: AssetExecutionContext):
    """
    Alternative cleanup asset using PyAirbyteCacheDBManager.from_cache_name() method.
    This demonstrates the class method approach for creating a manager with Sweden cache.
    """
    
    context.log.info("Starting Sweden cache cleanup using manager class method...")
    
    try:
        # Use the class method to create manager with Sweden cache
        db_manager = PyAirbyteCacheDBManager.from_cache_name("sweden")
        
        tables_to_drop = [
            '_airbyte_destination_state',
            '_airbyte_state', 
            '_airbyte_streams'
        ]
        
        dropped_tables = []
        failed_tables = []
        
        for table in tables_to_drop:
            context.log.info(f"Dropping table: {table}")
            success = db_manager.drop_cache_table(table)
            
            if success:
                dropped_tables.append(table)
                context.log.info(f"✓ Successfully dropped {table}")
            else:
                failed_tables.append(table)
                context.log.warning(f"✗ Failed to drop {table}")
        
        context.log.info(f"Sweden cache cleanup using manager completed. Dropped {len(dropped_tables)} tables.")
        
        # Return cleanup summary
        cleanup_result = {
            "status": "success",
            "dropped_tables": dropped_tables,
            "failed_tables": failed_tables,
            "total_dropped": len(dropped_tables),
            "total_failed": len(failed_tables),
            "cache_schema": db_manager.schema_name,
            "method": "PyAirbyteCacheDBManager.from_cache_name"
        }
        
        context.add_output_metadata({
            "status": MetadataValue.text("success"),
            "tables_dropped": MetadataValue.text(", ".join(dropped_tables)),
            "tables_failed": MetadataValue.text(", ".join(failed_tables)),
            "total_dropped": MetadataValue.int(len(dropped_tables)),
            "total_failed": MetadataValue.int(len(failed_tables)),
            "cache_schema": MetadataValue.text(db_manager.schema_name),
            "method": MetadataValue.text("PyAirbyteCacheDBManager.from_cache_name")
        })
        
        return cleanup_result
        
    except Exception as e:
        context.log.error(f"Error during Sweden cache cleanup using manager: {str(e)}")
        
        cleanup_result = {
            "status": "error",
            "error": str(e),
            "dropped_tables": [],
            "failed_tables": [],
            "total_dropped": 0,
            "total_failed": 0,
            "cache_schema": "sweden_cache_error",
            "method": "PyAirbyteCacheDBManager.from_cache_name"
        }
        
        context.add_output_metadata({
            "status": MetadataValue.text("error"),
            "error": MetadataValue.text(str(e))
        })
        
        # Don't fail the pipeline, just return error status
        return cleanup_result
