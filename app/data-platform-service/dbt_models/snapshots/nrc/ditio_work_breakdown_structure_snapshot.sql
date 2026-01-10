{% snapshot ditio_work_breakdown_structure_snapshot %}
{{
  config(
    target_schema='snapshots',
    unique_key='id',
    strategy='timestamp',
    updated_at='modifieddatetime::TIMESTAMP',
    hard_deletes='invalidate',
    tags='nrc'
  )
}}
SELECT 
    *
FROM {{ source('raw_nrc_source', 'source_ditio_work_breakdown_structure') }}
{% endsnapshot %}