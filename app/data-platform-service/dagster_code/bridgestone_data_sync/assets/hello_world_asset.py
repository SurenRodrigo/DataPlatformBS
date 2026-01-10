from dagster import asset, AssetExecutionContext


@asset(
    name="hello_world_asset",
    group_name="bridgestone_data_sync"
)
def hello_world_asset(context: AssetExecutionContext):
    """
    A simple hello world asset that prints 'hello world' to console.
    """
    message = "hello world"
    context.log.info(message)
    print(message)
    return message
