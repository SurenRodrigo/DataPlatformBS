{% snapshot icore_hours_snapshot %}
{{
  config(
    target_schema='snapshots',
    unique_key='recordid',
    strategy='timestamp',
    updated_at='alteredts::TIMESTAMP',
    hard_deletes='invalidate',
    tags='nrc'
  )
}}
SELECT 
    *
FROM {{ source('raw_nrc_source', 'source_icore_hours') }}
{% endsnapshot %}
