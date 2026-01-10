from dagster import define_asset_job
from ..assets import hello_world_asset

# Define the job for the hello world asset
bridgestone_data_sync_job = define_asset_job(
    name="bridgestone_data_sync_job",
    selection=[
        "hello_world_asset"
    ],
    description="Job to execute the Bridgestone data sync pipeline"
)
