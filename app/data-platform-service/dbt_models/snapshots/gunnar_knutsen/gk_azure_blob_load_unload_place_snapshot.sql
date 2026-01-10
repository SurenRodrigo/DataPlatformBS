{% snapshot gk_azure_blob_load_unload_place_snapshot %}

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
FROM {{ source('raw_nrc_source', 'source_azure_blob_load_un_load_place') }}

{% endsnapshot %}