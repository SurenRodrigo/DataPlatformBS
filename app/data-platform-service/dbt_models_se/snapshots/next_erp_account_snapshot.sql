{% snapshot next_erp_account_snapshot %}
{{
  config(
    target_schema='snapshots',
    unique_key='id',
    strategy='timestamp',
    updated_at='changed_timestamp'
  )
}}
SELECT 
    *,
    NULLIF(changed, '')::TIMESTAMP AS changed_timestamp
FROM {{ source('raw_se_source', 'next_erp_connector_account') }}
{% endsnapshot %}

