"""
Asset to sync Sales.Invoices table from Wide World Importers source database
to dataplatform destination database using MSSQL to MSSQL sync utility.
"""
import sys
import os
from dagster import asset, MetadataValue, AssetExecutionContext

# Add data-manager to Python path
sys.path.append('/app/data-manager')

from pyairbyte.utils.mssql_to_mssql_sync import sync_mssql_query_to_mssql


@asset(
    name="sync_wwi_invoices",
    group_name="bridgestone_data_sync",
    description="Sync Sales.Invoices table from Wide World Importers source database to dataplatform"
)
def sync_wwi_invoices(context: AssetExecutionContext):
    """
    Sync Wide World Importers Sales.Invoices table from source_data Azure MSSQL database
    to dataplatform Azure MSSQL database using Entra ID Service Principal authentication.
    
    Source: source_data.Sales.Invoices
    Destination: dataplatform.pyairbyte_cache.wwi_source_invoices
    
    The sync uses checksum-based deduplication to efficiently handle:
    - Insert new invoices
    - Update modified invoices
    - Skip unchanged invoices
    """
    
    context.log.info("=" * 80)
    context.log.info("Starting Wide World Importers Sales.Invoices sync")
    context.log.info("=" * 80)
    
    # Source: Wide World Importers database (source_data) with dedicated service principal
    source_config = {
        "server": os.getenv("AZURE_SQL_SERVER_FQDN", "dataplatformpoc.database.windows.net"),
        "database": os.getenv("AZURE_SOURCE_DATA_DATABASE_NAME", "source_data"),
        "client_id": os.getenv("AZURE_SOURCE_DATA_CLIENT_ID"),
        "client_secret": os.getenv("AZURE_SOURCE_DATA_CLIENT_SECRET"),
        "tenant_id": os.getenv("AZURE_SOURCE_DATA_TENANT_ID"),
        "port": os.getenv("AZURE_SQL_PORT", "1433")
    }
    
    # Destination: dataplatform database with dedicated service principal
    dest_config = {
        "server": os.getenv("AZURE_SQL_SERVER_FQDN", "dataplatformpoc.database.windows.net"),
        "database": os.getenv("AZURE_SQL_DATABASE_NAME", "dataplatform"),
        "client_id": os.getenv("AZURE_CLIENT_ID"),
        "client_secret": os.getenv("AZURE_CLIENT_SECRET"),
        "tenant_id": os.getenv("AZURE_TENANT_ID"),
        "port": os.getenv("AZURE_SQL_PORT", "1433")
    }
    
    # Validate required environment variables
    required_source_vars = ['AZURE_SOURCE_DATA_CLIENT_ID', 'AZURE_SOURCE_DATA_CLIENT_SECRET', 'AZURE_SOURCE_DATA_TENANT_ID']
    required_dest_vars = ['AZURE_CLIENT_ID', 'AZURE_CLIENT_SECRET', 'AZURE_TENANT_ID']
    
    missing_vars = []
    for var in required_source_vars + required_dest_vars:
        if not os.getenv(var):
            missing_vars.append(var)
    
    if missing_vars:
        error_msg = f"Missing required environment variables: {', '.join(missing_vars)}"
        context.log.error(error_msg)
        raise ValueError(error_msg)
    
    # SQL Query to extract Sales.Invoices from Wide World Importers
    # Select all columns from Sales.Invoices table
    query = """
    SELECT 
        InvoiceID,
        CustomerID,
        BillToCustomerID,
        OrderID,
        DeliveryMethodID,
        ContactPersonID,
        AccountsPersonID,
        SalespersonPersonID,
        PackedByPersonID,
        InvoiceDate,
        CustomerPurchaseOrderNumber,
        IsCreditNote,
        CreditNoteReason,
        Comments,
        DeliveryInstructions,
        InternalComments,
        TotalDryItems,
        TotalChillerItems,
        DeliveryRun,
        RunPosition,
        ReturnedDeliveryData,
        ConfirmedDeliveryTime,
        ConfirmedReceivedBy,
        LastEditedBy,
        LastEditedWhen
    FROM Sales.Invoices
    """
    
    context.log.info("Source Database: source_data (Wide World Importers)")
    context.log.info("Source Table: Sales.Invoices")
    context.log.info("Destination Database: dataplatform")
    context.log.info("Destination Schema: pyairbyte_cache")
    context.log.info("Destination Table: wwi_source_invoices")
    context.log.info(f"Query: {query[:100]}...")
    
    try:
        # Execute sync operation with streaming enabled for large datasets
        result = sync_mssql_query_to_mssql(
            source_config=source_config,
            source_query=query,
            dest_config=dest_config,
            dest_schema="pyairbyte_cache",
            dest_table="wwi_source_invoices",
            merge_key_columns=["InvoiceID"],  # Use InvoiceID as primary merge key
            batch_size=10000,
            validate_row_counts=True,
            chunk_size=1000,  # Read 1000 rows at a time
            max_workers=4,  # Process up to 4 chunks concurrently
            use_streaming=True  # Enable streaming for large datasets
        )
        
        # Log and handle result based on status
        if result['status'] == 'success':
            context.log.info("✓ Sync completed successfully")
            
            # Add comprehensive metadata
            metadata = {
                "status": MetadataValue.text("success"),
                "authentication": MetadataValue.text(result.get('authentication_method', 'unknown')),
                "rows_queried": MetadataValue.int(result['result']['rows_queried']),
                "rows_inserted": MetadataValue.int(result['result']['rows_inserted']),
                "rows_updated": MetadataValue.int(result['result']['rows_updated']),
                "rows_unchanged": MetadataValue.int(result['result']['rows_unchanged']),
                "processing_time": MetadataValue.float(result['result']['processing_time_seconds']),
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
        context.log.info("Wide World Importers Sales.Invoices sync completed")
        context.log.info("=" * 80)
        
        return result
    
    except Exception as e:
        context.log.error(f"Exception during sync: {type(e).__name__}")
        context.log.error(f"Exception message: {str(e)}")
        context.log.error("Exception traceback:", exc_info=True)
        raise
