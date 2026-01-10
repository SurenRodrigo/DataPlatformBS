{% snapshot unit4_invoice_unpaid_transaction_snapshot %}
{{
  config(
    target_schema='snapshots',
    unique_key='transactionnumber::TEXT || \'-\' || sequencenumber::TEXT || \'-\' || COALESCE(invoice ->> \'bacsId\', \'\')',
    strategy='timestamp',
    updated_at='lastupdatedat::TIMESTAMP',
    hard_deletes='new_record'
  )
}}
SELECT 
    *,
    (lastupdated ->> 'updatedAt')::TIMESTAMP AS lastupdatedat
FROM {{ source('raw_nrc_source', 'source_unit4_invoice_unpaid_transaction') }}
{% endsnapshot %}
