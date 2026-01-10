{% snapshot jobylon_job_snapshot %}
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
    id,
    title,
    status,
    to_date,
    from_date,
    is_hidden,
    job_owner,
    company_id,
    dt_created,
    dt_modified,
    is_internal,
    is_template,
    is_confidential,
    internal_reference
FROM {{ source('raw_nrc_source', 'source_jobylon_bi_job') }}
{% endsnapshot %}