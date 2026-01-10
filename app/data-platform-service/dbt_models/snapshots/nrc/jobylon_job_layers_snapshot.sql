{% snapshot jobylon_job_layers_snapshot %}
{{
  config(
    target_schema='snapshots',
    unique_key='job_layer_identifier',
    strategy='check',
    check_cols=['name','text','job_id','dimension','company_id','layerconfig_id','layeroption_id'],
    hard_deletes='invalidate'
  )
}}
SELECT
    *,
    {{ dbt_utils.generate_surrogate_key(['name', 'text', 'job_id', 'dimension', 'company_id', 'layerconfig_id', 'layeroption_id']) }} AS job_layer_identifier
FROM {{ source('raw_nrc_source', 'source_jobylon_bi_job_layers') }}
{% endsnapshot %}


