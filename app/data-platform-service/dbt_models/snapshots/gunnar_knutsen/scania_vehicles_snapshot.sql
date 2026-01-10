{% snapshot scania_vehicles_snapshot %}

{{
  config(
    target_schema='snapshots',
    unique_key='CONCAT("vin", "customervehiclename")',
    strategy='check',
    check_cols=['"vin"','"customervehiclename"'],
    tags=['gunnar_knutsen', 'scania']
  )
}}

SELECT * FROM {{ source('raw_nrc_source', 'source_scania_vehicles') }}

{% endsnapshot %}