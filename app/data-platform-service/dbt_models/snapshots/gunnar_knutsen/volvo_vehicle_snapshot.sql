{% snapshot volvo_vehicle_snapshot %}

{{
    config(
        target_schema='snapshots',
        unique_key='CONCAT("vin", "customervehiclename")',
        strategy='check',
        check_cols=['"vin"','"customervehiclename"'],
        tags=['gunnar_knutsen', 'volvo']
    )
}}

SELECT 
    *
FROM {{ source('raw_nrc_source', 'source_volvo_vehicles') }}

{% endsnapshot %}