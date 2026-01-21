"""
Asset to sync combined financial data (invoices + credits) from source_data database
to dataplatform database for reporting purposes.
"""
import sys
import os
from dagster import asset, MetadataValue, AssetExecutionContext

# Add data-manager to Python path
sys.path.append('/app/data-manager')

from pyairbyte.utils.mssql_to_mssql_sync import sync_mssql_query_to_mssql


@asset(
    name="sync_direct_fin_data",
    group_name="bridgestone_data_sync",
    deps=["sync_credit_data"],  # Execute after sync_credit_data
    description="Sync combined invoice and credit data from source_data to dataplatform reporting schema"
)
def sync_direct_fin_data(context: AssetExecutionContext):
    """
    Sync combined financial data (invoices and credits) from source_data Azure MSSQL database
    to dataplatform Azure MSSQL database for reporting purposes.
    
    This asset executes a UNION query that combines:
    - pyairbyte_cache.invoice_data (with positive amounts)
    - pyairbyte_cache.credit_data (with negated amounts for proper aggregation)
    
    Source: source_data.pyairbyte_cache.invoice_data + credit_data
    Destination: dataplatform.reporting.financial_reporting_data
    
    Uses Entra ID Service Principal authentication for both source and destination.
    """
    
    context.log.info("=" * 80)
    context.log.info("Starting Financial Reporting Data Sync")
    context.log.info("Combining invoice_data and credit_data into financial_reporting_data")
    context.log.info("=" * 80)
    
    # Source: source_data database with dedicated service principal
    source_config = {
        "server": os.getenv("AZURE_SQL_SERVER_FQDN"),
        "database": os.getenv("AZURE_SOURCE_DATA_DATABASE_NAME", "source_data"),
        "client_id": os.getenv("AZURE_SOURCE_DATA_CLIENT_ID"),
        "client_secret": os.getenv("AZURE_SOURCE_DATA_CLIENT_SECRET"),
        "tenant_id": os.getenv("AZURE_SOURCE_DATA_TENANT_ID"),
        "port": os.getenv("AZURE_SQL_PORT", "1433")
    }
    
    # Destination: dataplatform database with dedicated service principal
    dest_config = {
        "server": os.getenv("AZURE_SQL_SERVER_FQDN"),
        "database": os.getenv("AZURE_SQL_DATABASE_NAME", "dataplatform"),
        "client_id": os.getenv("AZURE_DATAPLATFORM_DATA_CLIENT_ID"),
        "client_secret": os.getenv("AZURE_DATAPLATFORM_DATA_CLIENT_SECRET"),
        "tenant_id": os.getenv("AZURE_DATAPLATFORM_DATA_TENANT_ID"),
        "port": os.getenv("AZURE_SQL_PORT", "1433")
    }
    
    # Validate required environment variables
    required_source_vars = [
        'AZURE_SQL_SERVER_FQDN',
        'AZURE_SOURCE_DATA_CLIENT_ID', 
        'AZURE_SOURCE_DATA_CLIENT_SECRET', 
        'AZURE_SOURCE_DATA_TENANT_ID'
    ]
    required_dest_vars = [
        'AZURE_DATAPLATFORM_DATA_CLIENT_ID', 
        'AZURE_DATAPLATFORM_DATA_CLIENT_SECRET', 
        'AZURE_DATAPLATFORM_DATA_TENANT_ID'
    ]
    
    missing_vars = []
    for var in required_source_vars + required_dest_vars:
        if not os.getenv(var):
            missing_vars.append(var)
    
    if missing_vars:
        error_msg = f"Missing required environment variables: {', '.join(missing_vars)}"
        context.log.error(error_msg)
        raise ValueError(error_msg)
    
    # SQL Query to combine invoice_data and credit_data
    # Credit data amounts are negated to allow proper aggregation (credits offset invoices)
    # NOTE: Do NOT include GETDATE() or any time-varying columns in the query!
    #       The checksum is calculated from ALL columns, so time-varying values
    #       would make every sync produce different checksums (causing duplicates).
    #       The MERGE operation automatically adds _sync_updated_at column.
    query = """
    SELECT 
        invoice_number,
        customer_name,
        item_code,
        seller_id,
        seller_name,
        quantity,
        line_total,
        gross_profit,
        customer_code,
        items_group_name,
        posted_date,
        end_of_month_bucket,
        item_category,
        year,
        customer_group,
        cohort,
        ipc,
        dim,
        'invoice' AS record_type
    FROM [pyairbyte_cache].[invoice_data]

    UNION ALL

    SELECT 
        invoice_number,
        customer_name,
        item_code,
        seller_id,
        NULL AS seller_name,
        quantity,
        -line_total AS line_total,
        -gross_profit AS gross_profit,
        customer_code,
        items_group_name,
        posted_date,
        end_of_month_bucket,
        item_category,
        year,
        customer_group,
        cohort,
        NULL AS ipc,
        NULL AS dim,
        'credit' AS record_type
    FROM [pyairbyte_cache].[credit_data]
    """
    
    context.log.info(f"Source Database: {source_config['database']}")
    context.log.info("Source Tables: pyairbyte_cache.invoice_data + pyairbyte_cache.credit_data")
    context.log.info(f"Destination Database: {dest_config['database']}")
    context.log.info("Destination Schema: reporting")
    context.log.info("Destination Table: financial_reporting_data")
    context.log.info("Using Entra ID Service Principal authentication")
    
    try:
        # Execute sync operation
        # Note: Using invoice_number + record_type + item_code as composite key for merge
        # This ensures unique identification of each row in the combined dataset
        # Using connection retry logic to handle Azure SQL transient failures:
        # - connection_retry_count=3: Retry up to 3 times on connection errors
        # - connection_retry_delay=5.0: Wait 5 seconds between retries
        result = sync_mssql_query_to_mssql(
            source_config=source_config,
            source_query=query,
            dest_config=dest_config,
            dest_schema="reporting",
            dest_table="financial_reporting_data",
            # merge_key_columns=["invoice_number", "record_type", "item_code", "customer_code"],
            batch_size=5000,
            validate_row_counts=True,
            chunk_size=2000,
            max_workers=4,
            use_streaming=True,
            connection_retry_count=3,
            connection_retry_delay=5.0
        )
        
        # Log and handle result based on status
        if result['status'] == 'success':
            context.log.info("✓ Financial reporting data sync completed successfully")
            
            # Add comprehensive metadata
            metadata = {
                "status": MetadataValue.text("success"),
                "authentication": MetadataValue.text(result.get('authentication_method', 'Entra ID Service Principal')),
                "rows_queried": MetadataValue.int(result['result']['rows_queried']),
                "rows_inserted": MetadataValue.int(result['result']['rows_inserted']),
                "rows_updated": MetadataValue.int(result['result']['rows_updated']),
                "rows_unchanged": MetadataValue.int(result['result']['rows_unchanged']),
                "processing_time": MetadataValue.float(result['result']['processing_time_seconds']),
                "source_database": MetadataValue.text(source_config['database']),
                "destination": MetadataValue.text(result['destination']),
                "checksum_column": MetadataValue.text(result['result']['checksum_column'])
            }
            
            # Add streaming-specific metadata
            if result['result'].get('streaming_enabled'):
                metadata["streaming_enabled"] = MetadataValue.text("Yes")
                metadata["chunks_processed"] = MetadataValue.int(result['result']['chunks_processed'])
                metadata["chunk_size"] = MetadataValue.int(result['result']['chunk_size'])
                metadata["max_workers"] = MetadataValue.int(result['result']['max_workers'])
            
            context.add_output_metadata(metadata)
            
            context.log.info(f"Rows queried: {result['result']['rows_queried']}")
            context.log.info(f"Rows inserted: {result['result']['rows_inserted']}")
            context.log.info(f"Rows updated: {result['result']['rows_updated']}")
            context.log.info(f"Rows unchanged: {result['result']['rows_unchanged']}")
            if result['result'].get('streaming_enabled'):
                context.log.info(f"Chunks processed: {result['result']['chunks_processed']} (concurrent)")
            context.log.info(f"Processing time: {result['result']['processing_time_seconds']}s")
        
        elif result['status'] == 'partial_success':
            context.log.warning("⚠ Sync completed with some errors")
            
            # Add metadata including error information
            context.add_output_metadata({
                "status": MetadataValue.text("partial_success"),
                "rows_queried": MetadataValue.int(result['result']['rows_queried']),
                "rows_inserted": MetadataValue.int(result['result']['rows_inserted']),
                "rows_updated": MetadataValue.int(result['result']['rows_updated']),
                "rows_skipped": MetadataValue.int(result['result']['rows_skipped']),
                "errors_count": MetadataValue.int(len(result['result']['error_summary'].get('first_10_errors', []))),
                "processing_time": MetadataValue.float(result['result']['processing_time_seconds'])
            })
            
            context.log.warning(f"Rows skipped due to errors: {result['result']['rows_skipped']}")
            context.log.warning(f"Error summary: {result['result']['error_summary']}")
        
        else:
            # Error status
            error_msg = result.get('error', 'Unknown error')
            context.log.error(f"✗ Sync failed: {error_msg}")
            raise Exception(f"Sync failed: {error_msg}")
        
        context.log.info("=" * 80)
        context.log.info("Financial Reporting Data Sync completed")
        context.log.info("=" * 80)
        
        return result
    
    except Exception as e:
        context.log.error(f"Exception during sync: {type(e).__name__}")
        context.log.error(f"Exception message: {str(e)}")
        context.log.error("Exception traceback:", exc_info=True)
        raise
