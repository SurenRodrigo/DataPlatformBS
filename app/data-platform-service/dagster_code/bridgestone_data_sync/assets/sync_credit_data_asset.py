import sys
import os
from dagster import asset, MetadataValue, AssetExecutionContext
from sqlalchemy.exc import SQLAlchemyError

# Add the data-manager path to sys.path for imports (container mount)
sys.path.append('/app/data-manager')

from pyairbyte.utils.excel_to_db_writer import ExcelToDbWriter


@asset(
    name="sync_credit_data",
    group_name="bridgestone_data_sync",
    deps=["sync_invoice_data"]  # Execute after sync_invoice_data
)
def sync_credit_data(context: AssetExecutionContext):
    """
    Dagster asset to sync credit data from Excel file to Azure SQL Database credit_data table.
    Reads from external Excel file and writes to credit_data table with field mapping.
    Uses Entra ID Service Principal authentication to connect to Azure SQL.
    Executes after sync_invoice_data asset.
    """
    context.log.info("Starting Bridgestone credit data sync from Excel to Azure SQL Database...")
    
    try:
        # Read Azure SQL connection configuration from environment variables
        # Server configuration
        azure_sql_server = os.getenv("AZURE_SQL_SERVER_FQDN")
        azure_sql_port = os.getenv("AZURE_SQL_PORT", "1433")
        
        # Database name (source_data)
        azure_db_name = os.getenv("AZURE_SOURCE_DATA_DATABASE_NAME")
        
        # Entra ID Service Principal credentials
        azure_client_id = os.getenv("AZURE_SOURCE_DATA_CLIENT_ID")
        azure_client_secret = os.getenv("AZURE_SOURCE_DATA_CLIENT_SECRET")
        azure_tenant_id = os.getenv("AZURE_SOURCE_DATA_TENANT_ID")
        
        # Validate required configuration
        required_vars = {
            "AZURE_SQL_SERVER_FQDN": azure_sql_server,
            "AZURE_SOURCE_DATA_DATABASE_NAME": azure_db_name,
            "AZURE_SOURCE_DATA_CLIENT_ID": azure_client_id,
            "AZURE_SOURCE_DATA_CLIENT_SECRET": azure_client_secret,
            "AZURE_SOURCE_DATA_TENANT_ID": azure_tenant_id
        }
        
        missing_vars = [k for k, v in required_vars.items() if not v]
        if missing_vars:
            raise ValueError(f"Missing required Azure SQL configuration: {missing_vars}")
        
        # Excel file path (mounted in container at /app/external_files)
        excel_path = "/app/external_files/brige_stone_source_credit_data_v3.xlsx"
        sheet_name = "CreditData"  # Assuming sheet name is "CreditData" - adjust if different
        
        # Database configuration
        schema_name = "pyairbyte_cache"
        table_name = "credit_data"
        
        # Field mapping from Excel columns to table columns
        field_mapping = {
            "Invoice_Number": "invoice_number",
            "Customer_Name": "customer_name",
            "Item_Code": "item_code",
            "Seller_Id": "seller_id",
            "Quantity": "quantity",
            "Line_Total": "line_total",
            "Gross_Profit": "gross_profit",
            "Customer_Code": "customer_code",
            "Items_Group_Name": "items_group_name",
            "Posted_Date": "posted_date",
            "End_Of_Month_Bucket": "end_of_month_bucket",
            "Item_Category": "item_category",
            "Year": "year",
            "Customer_Group": "customer_group",
            "Cohort": "cohort"
        }
        
        context.log.info(f"Azure SQL Server: {azure_sql_server}")
        context.log.info(f"Azure SQL Database: {azure_db_name}")
        context.log.info(f"Excel file path: {excel_path}")
        context.log.info(f"Sheet name: {sheet_name}")
        context.log.info(f"Target table: {schema_name}.{table_name}")
        context.log.info(f"Field mapping: {len(field_mapping)} columns")
        context.log.info("Using Entra ID Service Principal authentication")
        
        # Create Azure SQL connection config with Service Principal authentication
        connection_config = {
            "server": azure_sql_server,
            "port": azure_sql_port,
            "database": azure_db_name,
            "client_id": azure_client_id,
            "client_secret": azure_client_secret,
            "tenant_id": azure_tenant_id,
            "schema": schema_name
        }
        
        # Initialize ExcelToDbWriter for MSSQL with Entra ID authentication
        writer = ExcelToDbWriter(
            dbms_type="mssql",
            connection_config=connection_config,
            field_mapping=field_mapping
        )
        
        context.log.info("ExcelToDbWriter initialized successfully")
        
        # Write Excel data to database table
        result = writer.write_excel_to_table(
            excel_path=excel_path,
            sheet_name=sheet_name,
            schema_name=schema_name,
            table_name=table_name,
            chunk_size=10000,  # Process 10K rows per chunk
            if_exists="replace"  # replace existing data
        )
        
        # Check result status and handle accordingly
        status = result.get("status", "unknown")
        rows_written = result.get("rows_written", 0)
        chunks_processed = result.get("chunks_processed", 0)
        errors = result.get("errors", [])
        warnings = result.get("warnings", [])
        table_created = result.get("table_created", False)
        
        context.log.info(
            f"Excel to DB write completed: status={status}, "
            f"rows_written={rows_written}, chunks_processed={chunks_processed}, "
            f"table_created={table_created}, errors={len(errors)}, warnings={len(warnings)}"
        )
        
        if table_created:
            context.log.info(f"Table '{schema_name}.{table_name}' was automatically created")
        
        # Add detailed metadata
        context.add_output_metadata({
            "status": MetadataValue.text(status),
            "rows_written": MetadataValue.int(rows_written),
            "chunks_processed": MetadataValue.int(chunks_processed),
            "table_created": MetadataValue.bool(table_created),
            "errors_count": MetadataValue.int(len(errors)),
            "warnings_count": MetadataValue.int(len(warnings)),
            "database_type": MetadataValue.text("Azure SQL (MSSQL)"),
            "azure_server": MetadataValue.text(azure_sql_server),
            "azure_database": MetadataValue.text(azure_db_name),
            "auth_method": MetadataValue.text("Entra ID Service Principal"),
            "schema": MetadataValue.text(schema_name),
            "table": MetadataValue.text(table_name),
            "excel_file": MetadataValue.text(excel_path),
            "sheet": MetadataValue.text(sheet_name)
        })
        
        # Log errors if any
        if errors:
            context.log.warning(f"Encountered {len(errors)} errors during processing:")
            for error in errors[:10]:  # Log first 10 errors
                context.log.warning(
                    f"  Chunk {error.get('chunk', 'unknown')}: "
                    f"{error.get('error_type', 'Unknown')}: {error.get('error', 'Unknown error')}"
                )
            if len(errors) > 10:
                context.log.warning(f"  ... and {len(errors) - 10} more errors")
        
        # Raise exception for critical failures (status="error" means no data was written)
        # Note: The utility should already raise SQLAlchemyError for critical failures,
        # but we check here as a safety net
        if status == "error":
            error_summary = f"Failed to write any data to {schema_name}.{table_name}. "
            error_summary += f"{len(errors)} chunks failed. "
            if errors:
                first_error = errors[0].get("error", "Unknown error")
                error_summary += f"First error: {first_error}"
            context.log.error(error_summary)
            raise SQLAlchemyError(error_summary)
        
        # Warn for partial failures - log but don't fail if some data was written
        if status == "partial":
            error_rate = len(errors) / (chunks_processed + len(errors)) * 100
            warning_msg = (
                f"Partial success: {rows_written} rows written but {len(errors)} chunks failed "
                f"({error_rate:.1f}% error rate). Some data may be missing."
            )
            context.log.warning(warning_msg)
            warnings.append(warning_msg)
            
            # Fail the job if error rate is too high (>50%)
            if error_rate > 50:
                error_summary = (
                    f"Critical failure: Error rate {error_rate:.1f}% exceeds threshold. "
                    f"Only {rows_written} rows written, {len(errors)} chunks failed."
                )
                context.log.error(error_summary)
                raise SQLAlchemyError(error_summary)
        
        return {
            "status": status,
            "rows_written": rows_written,
            "chunks_processed": chunks_processed,
            "errors": errors,
            "warnings": warnings
        }
        
    except SQLAlchemyError as e:
        error_msg = f"Database Error: {str(e)}"
        context.log.error(error_msg)
        context.add_output_metadata({
            "status": MetadataValue.text("error"),
            "error": MetadataValue.text(error_msg),
            "error_type": MetadataValue.text("SQLAlchemyError")
        })
        raise  # Re-raise to fail the Dagster job
    except ValueError as e:
        error_msg = f"Configuration Error: {str(e)}"
        context.log.error(error_msg)
        context.add_output_metadata({
            "status": MetadataValue.text("error"),
            "error": MetadataValue.text(error_msg),
            "error_type": MetadataValue.text("ValueError")
        })
        raise  # Re-raise to fail the Dagster job
    except FileNotFoundError as e:
        error_msg = f"File Not Found Error: {str(e)}"
        context.log.error(error_msg)
        context.add_output_metadata({
            "status": MetadataValue.text("error"),
            "error": MetadataValue.text(error_msg),
            "error_type": MetadataValue.text("FileNotFoundError")
        })
        raise  # Re-raise to fail the Dagster job
    except Exception as e:
        error_msg = f"Unexpected Error during credit data sync: {type(e).__name__}: {str(e)}"
        context.log.error(error_msg)
        context.add_output_metadata({
            "status": MetadataValue.text("error"),
            "error": MetadataValue.text(error_msg),
            "error_type": MetadataValue.text(type(e).__name__)
        })
        raise  # Re-raise to fail the Dagster job
