{% snapshot unit4_liquidity_department_snapshot %}
{{
  config(
    target_schema='snapshots',
    unique_key = 'COALESCE(vo, \'NO_VO\') || \'-\' || period || \'-\' || department || \'-\' || foeht || \'-\' || clientid',
    strategy='check',
    check_cols=['amountacc']
  )
}}
SELECT 
    * 
FROM {{ source('raw_nrc_source', 'source_unit4_liquidity_department') }}
{% endsnapshot %}
