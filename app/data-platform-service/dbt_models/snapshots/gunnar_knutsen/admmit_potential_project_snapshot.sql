{% snapshot admmit_potential_project_snapshot %}
{{
  config(
    target_schema='snapshots',
    unique_key=' "External ID" ',
    strategy='check',
    check_cols='all',
    tags=['gunnar_knutsen', 'sharepoint']
  )
}}

SELECT
    *
FROM {{ source('raw_nrc_source', 'source_sharepoint_gk_potential_project_manual_data') }}

{% endsnapshot %}
