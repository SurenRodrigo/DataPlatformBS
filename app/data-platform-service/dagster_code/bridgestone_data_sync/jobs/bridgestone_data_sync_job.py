from dagster import define_asset_job
from ..assets import hello_world_asset, sync_invoice_data, sync_credit_data, sync_direct_fin_data

# Define the job for the Bridgestone data sync pipeline
bridgestone_data_sync_job = define_asset_job(
    name="bridgestone_data_sync_job",
    selection=[
        "sync_invoice_data",     # First: sync invoice data from Excel to source_data Azure database
        "sync_credit_data",      # Second: sync credit data from Excel to source_data Azure database
        "sync_direct_fin_data"   # Third: sync combined financial data to dataplatform reporting schema
    ],
    description="Job to execute the Bridgestone data sync pipeline - syncs invoice and credit data from Excel to Azure SQL, then combines into financial reporting table"
)
