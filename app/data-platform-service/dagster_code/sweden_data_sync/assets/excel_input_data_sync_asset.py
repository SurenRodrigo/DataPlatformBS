import sys
import os
import io
from dagster import asset, MetadataValue, AssetExecutionContext

# Add the data-manager path to sys.path for imports (container mount)
sys.path.append('/app/data-manager')

from pyairbyte.utils.sharepoint_client import SharePointGraphClient  # Updated to Graph client
from pyairbyte.utils.excel_reader import ExcelReader
from pyairbyte.utils.sql_writer import SqlWriter
from pyairbyte.utils.common_cache import get_cache

@asset(
    name="excel_input_data_sync_asset",
    group_name="sweden_data_sync",
    deps=["cleanup_airbyte_cache_tables_sweden"]
)
def excel_input_data_sync_asset(context: AssetExecutionContext):
    """
    Dagster asset to sync Excel input data from SharePoint to Sweden PostgreSQL cache.
    Data is automatically stored in pyairbyte_cache_sweden schema, ready for DBT processing.
    """
    context.log.info("Starting Excel input data sync from SharePoint to Sweden cache...")
    
    try:
        connector_name = "excel-input-connector"
        excel_input_data_sheet_names = ["Ktomap","Projmap","Anv","Maskiner","Projektmappning NY"]
        columns_to_extract = {
            "Anv": ["Anställningsnr","För- och efternamn","Inloggning","Primär grupp","Yrkesroll","Anställningsform","Namn","Startdatum","Slutdatum","Aktiv","Språk","Organisatoriskt projekt"]
        }
        # Get Sweden cache with connector-specific table prefix
        cache = get_cache("sweden", connector_name)
        context.log.info(f"Using Sweden cache with schema: {cache.schema_name}")
        
        # Read SharePoint configuration (business logic - read inside function for better testability)
        sp_client_id = os.getenv("SE_SP_CLIENT_ID")
        sp_client_secret = os.getenv("SE_SP_CLIENT_SECRET")
        se_host = os.getenv("SE_SP_HOSTNAME")
        se_site_path = os.getenv("SE_SP_SITE_PATH")
        se_drive_name = os.getenv("SE_SP_DRIVE_NAME")
        se_item_path = os.getenv("SE_SP_SYNC_FILE_PATH")
        
        # Validate required configuration
        if not all([sp_client_id, sp_client_secret, se_host, se_site_path, se_item_path]):
            raise ValueError("Missing required SharePoint configuration.")
        
        # ------------------------------------------------------------------
        # Graph client initialization (uses application permissions)
        # Expected site URL format: https://<hostname>/sites/<site_path>/...
        # ------------------------------------------------------------------
        tenant_id = os.getenv("SE_SP_TENANT_ID")
        if not tenant_id:
            raise ValueError("Missing tenant id: please set SE_SP_TENANT_ID (do not reuse GK tenant).")

        hostname = se_host
        site_path = se_site_path
        drive_name = se_drive_name or os.getenv("SE_SP_DEFAULT_DRIVE_NAME", "Documents")
        item_path = se_item_path.strip('/')
        context.log.info(f"Graph download target - host: {hostname}, site: {site_path}, drive: {drive_name}, item: {item_path}")

        graph_client = SharePointGraphClient(
            tenant_id=tenant_id,
            client_id=sp_client_id,
            client_secret=sp_client_secret
        )

        # Ensure drive name resolved
        if not drive_name:
            raise ValueError("Provide the document library name (e.g., 'Documents').")

        # Download file bytes via Microsoft Graph
        file_content = graph_client.download_file_bytes(
            hostname=hostname,
            site_path=site_path,
            drive_name=drive_name,
            item_path=item_path
        )
        # Convert to BytesIO for pandas
        file_bytes = io.BytesIO(file_content)
        context.log.info("File downloaded successfully")

        # Read Excel sheets
        reader = ExcelReader()

        # Read specific sheets - pass the BytesIO object
        sheets = reader.read_all_sheets(file_bytes, sheets=excel_input_data_sheet_names)
        context.log.info(f"Successfully read {len(sheets)} sheets")
    
        # Create SQL writer with cache object
        sql_writer = SqlWriter(connector_name, cache=cache)

        # Define columns to extract from employee sheet. So minimize GDPR risks.
        tables_written = []
        total_rows = 0
        
        for name, df in sheets.items():
            context.log.info(f"Processing sheet: {name}")
            
            # Filter columns only if sheet name equals "Anv"
            if name == "Anv":
                available_cols = [col for col in columns_to_extract["Anv"] if col in df.columns]
                df = df[available_cols]
                context.log.info(f"Filtered sheet '{name}' to {len(available_cols)} columns")
            
            tables = reader.extract_tables_by_blank_columns(df, name)
            context.log.info(f"Extracted {len(tables)} table(s) from sheet '{name}'")
            
            if not tables:
                context.log.warning(f"No tables extracted from sheet '{name}'")
                continue
            
            for t_name, t_df in tables.items():
                context.log.info(f"Extracted table: {t_name}, Rows: {len(t_df)}")
                context.log.info(f"Columns: {t_df.columns.tolist()}")
                
                # Write to SQL with schema specified
                sql_writer.write_df_to_table(t_df, t_name.lower(), if_exists='replace')
                tables_written.append(t_name.lower())
                total_rows += len(t_df)
                context.log.info(f"Successfully wrote table '{t_name}' to database")

        context.log.info("Excel input data successfully synced to Sweden PostgreSQL cache - ready for DBT processing")
        
        # Add success metadata
        context.add_output_metadata({
            "status": MetadataValue.text("success"),
            "cache_schema": MetadataValue.text(cache.schema_name),
            "connector": MetadataValue.text(connector_name),
            "cache_config": MetadataValue.text("sweden"),
            "tables_written": MetadataValue.text(", ".join(tables_written)),
            "total_rows": MetadataValue.int(total_rows),
            "sheets_processed": MetadataValue.int(len(sheets))
        })
        
        return {
            "status": "success",
            "cache_schema": cache.schema_name,
            "tables_written": tables_written,
            "total_rows": total_rows,
            "sheets_processed": len(sheets)
        }

    except ValueError as e:
        error_msg = f"Configuration Error: {str(e)}"
        context.log.error(error_msg)
        context.add_output_metadata({
            "status": MetadataValue.text("error"),
            "error": MetadataValue.text(error_msg),
            "connector": MetadataValue.text(connector_name),
            "cache_config": MetadataValue.text("sweden")
        })
        raise
    except Exception as e:
        error_msg = f"Error during Excel input data sync: {str(e)}"
        context.log.error(error_msg)
        context.add_output_metadata({
            "status": MetadataValue.text("error"),
            "error": MetadataValue.text(error_msg),
            "connector": MetadataValue.text(connector_name),
            "cache_config": MetadataValue.text("sweden")
        })
        raise