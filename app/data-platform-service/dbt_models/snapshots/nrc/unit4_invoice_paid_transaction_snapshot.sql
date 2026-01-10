{% snapshot unit4_invoice_paid_transaction_snapshot %}
{{
  config(
    target_schema='snapshots',
    unique_key='transactionnumber::TEXT || \'-\' || sequencenumber::TEXT || \'-\' || COALESCE(invoice ->> \'bacsId\', \'\')',
    strategy='timestamp',
    updated_at='lastupdatedat::TIMESTAMP'
  )
}}
SELECT 
    *,
    (lastupdated ->> 'updatedAt')::TIMESTAMP AS lastupdatedat
FROM {{ source('raw_nrc_source', 'source_unit4_invoice_paid_transaction') }}
{% endsnapshot %}
