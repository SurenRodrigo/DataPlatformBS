{% snapshot scania_vehicle_positions_snapshot %}
{{
  config(
    target_schema='snapshots',
    unique_key='CONCAT("vin", "createddatetime")',
    strategy='timestamp',
    updated_at='createddatetime::TIMESTAMP',
    tags=['gunnar_knutsen', 'scania']
  )
}}
SELECT
    *
FROM {{ source('raw_nrc_source', 'source_scania_vehicle_positions') }}
{% endsnapshot %}