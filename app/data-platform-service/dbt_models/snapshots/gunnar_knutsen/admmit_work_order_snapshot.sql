{% snapshot admmit_work_order_snapshot %}

{{
    config(
        target_schema='snapshots',
        unique_key='CONCAT("internalid", "clientid")',
        strategy='timestamp',
        updated_at='lastupdated',
        tags=['gunnar_knutsen', 'admmit']
    )
}}

SELECT
    *,
    COALESCE("modifiedon", "createdon") AS lastupdated
FROM {{ source('raw_nrc_source', 'source_admmit_WorkOrder') }}

{% endsnapshot %}