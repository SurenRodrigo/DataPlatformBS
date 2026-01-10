{% snapshot jobylon_application_status_log_snapshot %}
{{
  config(
    target_schema='snapshots',
    unique_key='id',
    strategy='timestamp',
    updated_at='dt_created::timestamp',
    hard_deletes='invalidate'
  )
}}
SELECT
    *
FROM {{ source('raw_nrc_source', 'source_jobylon_bi_status_log') }}
{% endsnapshot %}


