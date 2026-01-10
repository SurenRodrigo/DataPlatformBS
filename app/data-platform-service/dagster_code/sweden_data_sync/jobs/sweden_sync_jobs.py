from dagster import define_asset_job
from ..assets import (
    cleanup_airbyte_cache_tables_sweden,
    next_erp_sync_assets_sweden,
    excel_input_data_sync_asset,
    dbt_setup_se,
    dbt_snapshots_se,
    dbt_seed_se,
    dbt_run_se,
    dbt_clean_se,
)

# Define the Sweden sync job - runs cleanup first, then syncs both connectors in parallel
sweden_sync_job = define_asset_job(
    name="sweden_sync_job",
    selection=[
        # First: Clean up Sweden cache tables
        "cleanup_airbyte_cache_tables_sweden",
        # Then: Sync both connectors in parallel (they both depend on cleanup)
        "next_erp_sync_assets_sweden",
        "excel_input_data_sync_asset",
        "dbt_setup_se",
        "dbt_snapshots_se",
        "dbt_seed_se",
        "dbt_run_se",
        "dbt_clean_se",
    ],
    description="Sweden sync job: Cleanup Sweden cache tables, then sync next-erp-connector and excel-input-connector in parallel using Sweden cache configuration and execute dbt pipeline.",
)

