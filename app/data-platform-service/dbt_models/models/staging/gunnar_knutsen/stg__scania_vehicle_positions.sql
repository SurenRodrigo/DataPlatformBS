SELECT
    "vin" AS vin,

    "triggertype"::jsonb ->> 'context' AS trigger_type_context,
    "triggertype"::jsonb ->> 'triggerType' AS triggerType,

    CASE 
      WHEN jsonb_typeof("triggertype"::jsonb -> 'driverId') = 'object'
      THEN "triggertype"::jsonb -> 'driverId' -> 'oemDriverIdentification' ->> 'idType'
      ELSE NULL
    END AS oem_driver_id_type,

    CASE 
      WHEN jsonb_typeof("triggertype"::jsonb -> 'driverId') = 'object'
      THEN "triggertype"::jsonb -> 'driverId' -> 'oemDriverIdentification' ->> 'oemDriverIdentification'
      ELSE NULL
    END AS oem_driver_id,

    CASE 
      WHEN jsonb_typeof("triggertype"::jsonb -> 'driverId') = 'object'
      THEN "triggertype"::jsonb -> 'driverId' -> 'tachoDriverIdentification' ->> 'driverIdentification'
      ELSE NULL
    END AS tacho_driver_id,

    CASE 
      WHEN jsonb_typeof("triggertype"::jsonb -> 'driverId') = 'object'
      THEN "triggertype"::jsonb -> 'driverId' -> 'tachoDriverIdentification' ->> 'cardIssuingMemberState'
      ELSE NULL
    END AS tacho_driver_card_issuer,

    "gnssposition" ->> 'latitude'         AS latitude,
    "gnssposition" ->> 'longitude'        AS longitude,
    ("gnssposition" ->> 'heading')::int   AS heading,
    ("gnssposition" ->> 'altitude')::int  AS altitude,
    ("gnssposition" ->> 'speed')::float   AS speedGnss,
    ("gnssposition" ->> 'positionDateTime')::timestamp AS position_datetime,

    "createddatetime"::timestamp          AS created_datetime,
    "receiveddatetime"::timestamp         AS received_datetime,

    "wheelbasedspeed"::float              AS wheel_based_speed,

    dbt_scd_id,
    dbt_updated_at,
    dbt_valid_from,
    dbt_valid_to

FROM {{ ref('scania_vehicle_positions_snapshot') }}
WHERE dbt_valid_to IS NULL
