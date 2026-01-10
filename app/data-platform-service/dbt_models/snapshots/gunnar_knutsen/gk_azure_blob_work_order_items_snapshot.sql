{% snapshot gk_azure_blob_work_order_items_snapshot %}

{{
    config(
        target_schema='snapshots',
        unique_key=' "Id" ',
        strategy='timestamp',
        updated_at='lastupdated',
        tags=['gunnar_knutsen', 'azure_blob']
    )
}}

SELECT
    *,
    COALESCE("ModifiedOn", "CreatedOn") AS lastupdated
FROM {{ source('raw_nrc_source', 'source_azure_blob_work_order_items') }}

{% endsnapshot %}