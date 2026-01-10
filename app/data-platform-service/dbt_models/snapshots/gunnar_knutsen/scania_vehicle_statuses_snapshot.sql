{% snapshot scania_vehicle_statuses_snapshot %}
{{
  config(
    target_schema='snapshots',
    unique_key='"vin" || \'_\' || "createddatetime"',
    strategy='timestamp',
    updated_at='createddatetime::TIMESTAMP',
    tags=['gunnar_knutsen', 'scania']
  )
}}
SELECT
    *
FROM {{ source('raw_nrc_source', 'source_scania_vehicle_statuses') }}
{% endsnapshot %}