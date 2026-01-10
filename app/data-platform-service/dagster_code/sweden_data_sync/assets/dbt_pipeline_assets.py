from dagster import asset, AssetExecutionContext
from dagster_dbt import DbtCliResource

"""
1. Check & install dbt Dependencies and parse dbt project
"""


@asset(
    name="dbt_setup_se",
    group_name="sweden_data_sync",
    deps=["next_erp_sync_assets_sweden", "excel_input_data_sync_asset"],
)
def dbt_setup_se(context: AssetExecutionContext, dbt: DbtCliResource):
    context.log.info("Installing DBT package dependencies (dbt deps)...")
    deps_result = dbt.cli(["deps"]).wait()
    if not deps_result.is_successful():
        raise Exception("DBT dependencies installation failed sweden_data_sync")
    context.log.info("Generating DBT manifest (dbt parse)...")
    parse_result = dbt.cli(["parse"]).wait()
    if not parse_result.is_successful():
        raise Exception("DBT parse failed")
    return {"status": "dbt_initialization_complete"}


"""
2. DBT snapshots
"""


@asset(
    name="dbt_snapshots_se",
    group_name="sweden_data_sync",
    deps=["dbt_setup_se"],
)
def dbt_snapshots_se(context: AssetExecutionContext, dbt: DbtCliResource):
    context.log.info("Executing DBT snapshots...")
    try:
        # Run snapshot without streaming (safer method)
        snapshots_result = dbt.cli(["snapshot", "--threads", "6"]).wait()
        
        if not snapshots_result.is_successful():
            error_message = getattr(snapshots_result.failure_event.raw, 'message', 'Unknown error')
            context.log.error(f"DBT Snapshot failed: {error_message}")
            raise Exception(f"DBT snapshots execution failed sweden_data_sync: {error_message}")
        
        context.log.info("DBT Snapshots completed successfully")
        return {"status": "dbt_snapshots_execution_completed"}
    except Exception as e:
        context.log.error(f"Error during DBT snapshots: {str(e)}")
        raise


"""
3. DBT seed
"""


@asset(
    name="dbt_seed_se",
    group_name="sweden_data_sync",
    deps=["dbt_snapshots_se"],
)
def dbt_seed_se(context: AssetExecutionContext, dbt: DbtCliResource):
    context.log.info("Executing DBT seed...")
    seed_result = dbt.cli(["seed", "-f"]).wait()
    if not seed_result.is_successful():
        raise Exception("DBT seed execution failed sweden_data_sync")
    return {"status": "dbt_seed_execution_completed"}


"""
4. DBT run
"""


@asset(
    name="dbt_run_se",
    group_name="sweden_data_sync",
    deps=["dbt_seed_se"],
)
def dbt_run_se(context: AssetExecutionContext, dbt: DbtCliResource):
    context.log.info("Executing DBT run...")
    execution_result = dbt.cli(["run", "--threads", "8"]).wait()
    if not execution_result.is_successful():
        raise Exception("DBT run failed for sweden_data_sync")
    return {"status": "dbt_run_execution_completed"}


"""
5. DBT clean
"""


@asset(
    name="dbt_clean_se",
    group_name="sweden_data_sync",
    deps=["dbt_run_se"],
)
def dbt_clean_se(context: AssetExecutionContext, dbt: DbtCliResource):
    context.log.info("Executing DBT clean...")
    result = dbt.cli(["clean"]).wait()
    if not result.is_successful():
        raise Exception("DBT clean execution failed sweden_data_sync")
    return {"status": "dbt_clean_execution_completed"}