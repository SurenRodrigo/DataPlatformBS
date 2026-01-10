{% snapshot unit4_ebit_snapshot %}
{{
  config(
    target_schema='snapshots',
    unique_key='clientid || period || projectname || project',
    strategy='check',
    check_cols=['accebit','ytdebit', 'periodebit','accRevenue','periodRevenue','ytdRevenue']
  )
}}
SELECT 
    * 
FROM {{ source('raw_nrc_source', 'source_unit4_ebit') }}
{% endsnapshot %}
