{% snapshot jobylon_application_snapshot %}
{{
  config(
    target_schema='snapshots',
    unique_key='id',
    strategy='timestamp',
    updated_at='dt_modified::timestamp',
    hard_deletes='invalidate'
  )
}}
SELECT
    *
FROM {{ source('raw_nrc_source', 'source_jobylon_bi_application') }}
{% endsnapshot %}


