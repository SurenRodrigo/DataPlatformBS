"""
Job to sync Wide World Importers data from source Azure MSSQL to destination Azure MSSQL.
"""
from dagster import define_asset_job
from ..assets import sync_wwi_invoices

# Define the job for syncing WWI data
sync_data_job = define_asset_job(
    name="sync_data_job",
    selection=[
        "sync_wwi_invoices"  # Sync Sales.Invoices from source_data to dataplatform
    ],
    description="Job to sync Wide World Importers Sales.Invoices table from source_data to dataplatform using MSSQL-to-MSSQL sync utility"
)
