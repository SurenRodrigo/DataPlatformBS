SELECT
    "vin"                            AS vin,
    "type"                           AS type,
    "brand"                          AS brand,
    "model"                          AS model,
    "noofaxles"                      AS no_of_axles,
    "chassistype"                    AS chassis_type,
    "gearboxtype"                    AS gearbox_type,
    "emissionlevel"                  AS emission_level,
    "hasramporlift"                  AS has_ramp_or_lift,
    ("productiondate" ->> 'day')     AS production_day,
    ("productiondate" ->> 'month')   AS production_month,
    ("productiondate" ->> 'year')   AS production_year,
    "tachographtype"                AS tachograph_type,
    "authorizedpaths"               AS authorized_paths,
    "possiblefueltype"              AS possible_fuel_type,
    "customervehiclename"           AS customer_vehicle_name,
    "totalfueltankvolume"           AS total_fuel_tank_volume,

    dbt_scd_id,
    dbt_updated_at,
    dbt_valid_from,
    dbt_valid_to

FROM {{ ref('scania_vehicles_snapshot') }}
WHERE dbt_valid_to IS NULL
