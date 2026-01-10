{% snapshot jobylon_job_posting_snapshot %}
{{
  config(
    target_schema='snapshots',
    unique_key='id',
    strategy='timestamp',
    updated_at='dt_modified::timestamp',
    tags=['nrc', 'jobylon_sync_tables'],
    invalidate_hard_deletes=true
  )
}}
SELECT 
    *
FROM {{ source('raw_nrc_source', 'source_jobylon_jobs') }}
{% endsnapshot %}