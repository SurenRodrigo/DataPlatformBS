{% snapshot gk_azure_blob_projects_snapshot %}

{{
    config(
        target_schema='snapshots',
        unique_key=' "Id" ',
        strategy='check',
        check_cols=['"Id"','"ExternalId"', '"ProjectNumber"'],
        tags=['gunnar_knutsen', 'azure_blob']
    )
}}

SELECT
    *
FROM {{ source('raw_nrc_source', 'source_azure_blob_projects') }}

{% endsnapshot %}