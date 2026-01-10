{% snapshot unit4_customer_snapshot %}
{{
  config(
    target_schema='snapshots',
    unique_key='customerid',
    strategy='timestamp',
    updated_at='last_updated::TIMESTAMP'
  )
}}
SELECT 
    *,
    lastupdated ->> 'updatedAt' AS last_updated
FROM {{ source('raw_nrc_source', 'source_unit4_customer') }}
{% endsnapshot %}