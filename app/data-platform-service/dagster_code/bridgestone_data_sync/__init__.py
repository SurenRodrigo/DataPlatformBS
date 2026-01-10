from dagster import Definitions

# Import assets and jobs
from .assets import hello_world_asset
from .jobs import bridgestone_data_sync_job

defs = Definitions(
    assets=[hello_world_asset],
    jobs=[bridgestone_data_sync_job],
    schedules=[],
)
