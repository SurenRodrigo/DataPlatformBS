{% snapshot unit4_project_snapshot %}
{{
  config(
    target_schema='snapshots',
    unique_key='projectid || companyid',
    strategy='timestamp',
    updated_at='last_updated_at::TIMESTAMP'
  )
}}
SELECT 
    *,
    lastupdated ->> 'updatedAt' AS last_updated_at
FROM {{ source('raw_nrc_source', 'source_unit4_project') }}
{% endsnapshot %}