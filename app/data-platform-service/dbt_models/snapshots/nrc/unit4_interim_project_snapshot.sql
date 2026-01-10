{% snapshot unit4_interim_project_snapshot %}
{{
  config(
    target_schema='snapshots',
    unique_key = 'COALESCE(vo, \'NO_VO\') || \'-\' || period || \'-\' || amounttype || \'-\' || project || \'-\' || department || \'-\' || foeht || \'-\' || clientid',
    strategy='check',
    check_cols=['amountacc', 'amountyear']
  )
}}
SELECT 
    * 
FROM {{ source('raw_nrc_source', 'source_unit4_interim_project') }}
{% endsnapshot %}
