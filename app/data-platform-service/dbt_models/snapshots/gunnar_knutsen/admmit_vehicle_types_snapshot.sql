{% snapshot admmit_order_item_vehicle_type_snapshot %}

{{
    config(
        target_schema='snapshots',
        unique_key=' "internalid" ',
        strategy='timestamp',
        updated_at='LastUpdated',
        tags=['gunnar_knutsen', 'admmit']
    )
}}

SELECT
    *,
    COALESCE("modifiedon", "createdon") AS lastupdated
FROM {{ source('raw_nrc_source', 'source_admmit_OrderItemVehicleType') }}

{% endsnapshot %}