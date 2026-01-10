{% snapshot manual_work_order_item_snapshot %}
{{
  config(
    target_schema='snapshots',
    unique_key=' "Project Id" ||  "Vehicle" ||  "Date" || "Time" || "Item" ' ,
    strategy='check',
    check_cols=['"Project Id"','"Vehicle"', '"Date"', '"Time"', '"Item"'],
    tags=['gunnar_knutsen', 'sharepoint']
  )
}}
SELECT * FROM {{ source('raw_nrc_source', 'source_sharepoint_gk_manual_data') }}
{% endsnapshot %}
