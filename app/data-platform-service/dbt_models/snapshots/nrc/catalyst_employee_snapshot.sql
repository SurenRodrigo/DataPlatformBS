{% snapshot catalyst_employee_snapshot %}
{{
  config(
    target_schema='snapshots',
    unique_key='guid',
    strategy='timestamp',
    updated_at='lastModified::TIMESTAMP'
  )
}}
SELECT 
    *
FROM {{ source('raw_nrc_source', 'source_catalyst_employee') }}
{% endsnapshot %}