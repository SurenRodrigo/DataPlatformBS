SELECT
    vin,
    type,
    brand,
    model,
    emissionlevel AS emission_level,
    ("productiondate" ->> 'Day')   AS production_day,
    ("productiondate" ->> 'Month') AS production_month,
    ("productiondate" ->> 'Year')  AS production_year,
    customervehiclename        AS customer_vehicle_name,
    dbt_scd_id,
    dbt_updated_at,
    dbt_valid_from,
    dbt_valid_to
FROM {{ ref('volvo_vehicle_snapshot') }}
WHERE dbt_valid_to IS NULL
