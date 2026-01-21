from dagster import Definitions

# Import assets and jobs
from .assets import sync_invoice_data, sync_credit_data, sync_direct_fin_data
from .jobs import bridgestone_data_sync_job

defs = Definitions(
    assets=[sync_invoice_data, sync_credit_data, sync_direct_fin_data],
    jobs=[bridgestone_data_sync_job],
    schedules=[],
)
