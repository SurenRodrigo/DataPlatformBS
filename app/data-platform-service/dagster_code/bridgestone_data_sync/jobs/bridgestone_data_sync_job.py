from dagster import define_asset_job
from ..assets import hello_world_asset, sync_invoice_data, sync_credit_data

# Define the job for the Bridgestone data sync pipeline
bridgestone_data_sync_job = define_asset_job(
    name="bridgestone_data_sync_job",
    selection=[
        "sync_invoice_data",  # First: sync invoice data from Excel to database
        "sync_credit_data"    # Second: sync credit data from Excel to database (executes after sync_invoice_data)
    ],
    description="Job to execute the Bridgestone data sync pipeline - syncs invoice and credit data from Excel to PostgreSQL"
)
