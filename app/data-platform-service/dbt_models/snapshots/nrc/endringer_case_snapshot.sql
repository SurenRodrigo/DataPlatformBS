{% snapshot endringer_case_snapshot %}
{{
  config(
    target_schema='snapshots',
    unique_key='id',
    strategy='timestamp',
    updated_at='modifieddate'
  )
}}
SELECT
    *
FROM {{ source('raw_nrc_source', 'source_endringer_case') }}
{% endsnapshot %}
