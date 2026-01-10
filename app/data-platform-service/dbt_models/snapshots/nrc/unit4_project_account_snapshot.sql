{% snapshot unit4_project_account_snapshot %}
{{
  config(
    target_schema='snapshots',
    unique_key='client || period || account || project',
    strategy='check',
    check_cols=['amountacc','amountytd', 'amountperiod']
  )
}}
SELECT 
    * 
FROM {{ source('raw_nrc_source', 'source_unit4_project_account') }}
{% endsnapshot %}
