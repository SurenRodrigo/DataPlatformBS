{% snapshot ditio_payroll_lines_snapshot %}
{{
  config(
    target_schema='snapshots',
    unique_key='id',
    strategy='timestamp',
    updated_at='modifieddatetime::TIMESTAMP',
    hard_deletes='invalidate'
  )
}}
SELECT 
    *
FROM {{ source('raw_nrc_source', 'source_ditio_payroll_lines') }}
{% endsnapshot %}