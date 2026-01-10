WITH fuel_types AS (
    SELECT
        "Reg.no" AS registration_number,
        "Fuel"   AS possible_fuel_type
    FROM {{ ref('fuel_type_seed') }}
),

admmit AS (
    SELECT * FROM {{ ref('stg__admmit_vehicle') }}
),

volvo AS (
    SELECT * FROM {{ ref('stg__volvo_vehicles') }}
),

scania AS (
    SELECT * FROM {{ ref('stg__scania_vehicles') }}
),

volvo_resources AS (
    SELECT DISTINCT ON (volvo.vin)
        volvo.vin                   AS resource_id,
        2                           AS tenant_id,
        'Gunnar Knutsen'            AS tenant_name,
        6                           AS data_source_id,
        'Volvo'                     AS data_source_name,
        volvo.vin,
        admmit.name,
        volvo.type,
        admmit.registration_number,
        volvo.customer_vehicle_name,
        volvo.brand,
        COALESCE(volvo.model, admmit.model) AS model,
        NULL::text              AS gearbox_type,
        volvo.emission_level,
        NULL::int               AS no_of_axles,
        COALESCE(fuel_types.possible_fuel_type, NULL::text) AS possible_fuel_type,
        NULL::numeric           AS total_fuel_tank_volume,
        admmit.client_id,
        admmit.hanger_id,
        admmit.is_active,
        admmit.is_hanger,  
        admmit.created_on,
        admmit.is_deleted,
        admmit.employee_id,
        admmit.internal_id,
        admmit.modified_on,
        admmit.description,
        admmit.display_name,
        admmit.department_id,
        admmit.vehicle_type_id,
        admmit.department_name,
        admmit.vehicle_type_name
    FROM volvo
    INNER JOIN admmit ON volvo.vin = admmit.vin
    LEFT JOIN fuel_types ON admmit.registration_number = fuel_types.registration_number
    WHERE volvo.vin IS NOT NULL 
      AND TRIM(volvo.vin) != ''
    ORDER BY volvo.vin, admmit.modified_on DESC NULLS LAST, admmit.created_on DESC NULLS LAST
),

scania_resources AS (
    SELECT DISTINCT ON (scania.vin)
        scania.vin                  AS resource_id,
        2                           AS tenant_id,
        'Gunnar Knutsen'            AS tenant_name,
        7                           AS data_source_id,
        'Scania'                    AS data_source_name,
        scania.vin,
        admmit.name,
        scania.type,
        admmit.registration_number,
        scania.customer_vehicle_name,
        scania.brand,
        COALESCE(scania.model, admmit.model) AS model,
        scania.gearbox_type,
        scania.emission_level,
        scania.no_of_axles,
        COALESCE(scania.possible_fuel_type::text, fuel_types.possible_fuel_type) AS possible_fuel_type,
        scania.total_fuel_tank_volume,
        admmit.client_id,
        admmit.hanger_id,
        admmit.is_active,
        admmit.is_hanger,
        admmit.created_on,
        admmit.is_deleted,
        admmit.employee_id,
        admmit.internal_id,
        admmit.modified_on,
        admmit.description,
        admmit.display_name,
        admmit.department_id,
        admmit.vehicle_type_id,
        admmit.department_name,
        admmit.vehicle_type_name
    FROM scania
    INNER JOIN admmit ON scania.vin = admmit.vin
    LEFT JOIN fuel_types ON admmit.registration_number = fuel_types.registration_number
    WHERE scania.vin IS NOT NULL 
      AND TRIM(scania.vin) != ''
    ORDER BY scania.vin, admmit.modified_on DESC NULLS LAST, admmit.created_on DESC NULLS LAST
),

admmit_only_resources AS (
    SELECT DISTINCT ON (admmit.vin)
        admmit.vin                  AS resource_id,
        2                           AS tenant_id,
        'Gunnar Knutsen'            AS tenant_name,
        8                           AS data_source_id,
        'Admmit'                    AS data_source_name,
        admmit.vin,
        admmit.name,
        NULL::text                  AS type,
        admmit.registration_number,
        NULL::text                  AS customer_vehicle_name,
        admmit.brand,
        admmit.model,
        NULL::text                  AS gearbox_type,
        NULL::text                  AS emission_level,
        NULL::int                   AS no_of_axles,
        fuel_types.possible_fuel_type,
        NULL::numeric               AS total_fuel_tank_volume,
        admmit.client_id,
        admmit.hanger_id,
        admmit.is_active,
        admmit.is_hanger,
        admmit.created_on,
        admmit.is_deleted,
        admmit.employee_id,
        admmit.internal_id,
        admmit.modified_on,
        admmit.description,
        admmit.display_name,
        admmit.department_id,
        admmit.vehicle_type_id,
        admmit.department_name,
        admmit.vehicle_type_name
    FROM admmit
    LEFT JOIN fuel_types ON admmit.registration_number = fuel_types.registration_number
    WHERE admmit.vin IS NOT NULL 
      AND TRIM(admmit.vin) != ''
      AND admmit.vin NOT IN (
        SELECT vin FROM volvo WHERE vin IS NOT NULL
        UNION
        SELECT vin FROM scania WHERE vin IS NOT NULL
    )
    ORDER BY admmit.vin, admmit.modified_on DESC NULLS LAST, admmit.created_on DESC NULLS LAST
)

SELECT * FROM volvo_resources
UNION ALL
SELECT * FROM scania_resources  
UNION ALL
SELECT * FROM admmit_only_resources
