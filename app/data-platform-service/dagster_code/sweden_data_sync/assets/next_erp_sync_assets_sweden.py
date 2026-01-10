import sys
import os
from dagster import asset, MetadataValue, AssetExecutionContext

# Add the data-manager path to sys.path for imports
sys.path.append('/app/data-manager')

from pyairbyte.utils.pyairbyte_sync import sync_connector
from pyairbyte.utils.common_cache import get_cache

@asset(
    name="next_erp_sync_assets_sweden",
    group_name="sweden_data_sync",
    deps=["excel_input_data_sync_asset"]
)
def next_erp_sync_assets_sweden(context):
    """
    Dagster asset to sync data from next-erp-connector using PyAirbyte with Sweden PostgreSQL cache.
    Data is automatically stored in pyairbyte_cache_sweden schema, ready for DBT processing.
    """
    context.log.info("Starting next-erp-connector sync with Sweden cache...")
    
    try:
        # Get Sweden cache with connector-specific table prefix
        cache = get_cache("sweden", "next_erp_connector")
        context.log.info(f"Using Sweden cache with schema: {cache.schema_name}")
        
        # Use the cache with sync_connector
        result = sync_connector("next_erp_connector", cache=cache)
        context.log.info(f"Sync result: {result}")
        
        if result.get('status') == 'success':
            context.add_output_metadata({
                "status": MetadataValue.text("success"),
                "cache_type": MetadataValue.text(result.get('cache_type', 'PostgresCache')),
                "cache_schema": MetadataValue.text(result.get('cache_schema', 'pyairbyte_cache_sweden')),
                "connector": MetadataValue.text("next_erp_connector"),
                "cache_config": MetadataValue.text("sweden"),
                "details": MetadataValue.json(result['result'])
            })
            context.log.info("Data successfully synced to Sweden PostgreSQL cache - ready for DBT processing")
        else:
            context.add_output_metadata({
                "status": MetadataValue.text("error"),
                "error": MetadataValue.text(result.get('error', 'Unknown error')),
                "cache_type": MetadataValue.text(result.get('cache_type', 'PostgresCache')),
                "cache_schema": MetadataValue.text(result.get('cache_schema', 'pyairbyte_cache_sweden')),
                "connector": MetadataValue.text("next_erp_connector"),
                "cache_config": MetadataValue.text("sweden")
            })
            context.log.error(f"Sync failed: {result.get('error', 'Unknown error')}")
            raise Exception(f"PyAirbyte sync failed: {result.get('error', 'Unknown error')}")

        return result
        
    except Exception as e:
        context.log.error(f"Error during next_erp_connector sync with Sweden cache: {str(e)}")
        raise
