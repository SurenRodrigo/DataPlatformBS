from dagster import Definitions

# Import assets and jobs
from .assets import hello_world_asset, sync_invoice_data, sync_credit_data, sync_wwi_invoices, sync_direct_fin_data
from .jobs import bridgestone_data_sync_job, sync_data_job

defs = Definitions(
    assets=[hello_world_asset, sync_invoice_data, sync_credit_data, sync_wwi_invoices, sync_direct_fin_data],
    jobs=[bridgestone_data_sync_job, sync_data_job],
    schedules=[],
)
