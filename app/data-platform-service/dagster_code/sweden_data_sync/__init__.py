from dagster import Definitions
from dagster_dbt import DbtCliResource

# Import assets and jobs from the new package structure
from .assets import (
    cleanup_airbyte_cache_tables_sweden,
    next_erp_sync_assets_sweden,
    excel_input_data_sync_asset,
    dbt_setup_se,
    dbt_snapshots_se,
    dbt_seed_se,
    dbt_run_se,
    dbt_clean_se,
)
from .jobs import sweden_sync_job

# DBT configuration
DBT_PROJECT_DIR = "/app/dbt_models_se"
DBT_PROFILES_DIR = "/app/dbt_models_se"


# Define the dbt CLI resource
resources = {
    "dbt": DbtCliResource(
        project_dir=DBT_PROJECT_DIR,
        profiles_dir=DBT_PROFILES_DIR,
    )
}

defs = Definitions(
    assets=[
        cleanup_airbyte_cache_tables_sweden,
        next_erp_sync_assets_sweden,
        excel_input_data_sync_asset,
        dbt_setup_se,
        dbt_snapshots_se,
        dbt_seed_se,
        dbt_run_se,
        dbt_clean_se,
    ],
    jobs=[sweden_sync_job],
    resources=resources,
    schedules=[],
)
