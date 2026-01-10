{% snapshot volvo_vehicle_status_snapshot %}
{{
  config(
    target_schema='snapshots',
    unique_key='CONCAT("vin", "createddatetime")',
    strategy='timestamp',
    updated_at='lastupdated::TIMESTAMP',
    tags=['gunnar_knutsen', 'volvo']
  )
}}
SELECT 
    *,
    COALESCE("createddatetime", "receiveddatetime") AS LastUpdated
FROM {{ source('raw_nrc_source', 'source_volvo_vehicle_statuses') }}
{% endsnapshot %}