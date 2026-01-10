SELECT
    "vin" AS vin,
    "triggertype" ->> 'TriggerType'       AS trigger_type,
    "triggertype" ->> 'Context'           AS trigger_context,
    "createddatetime"::timestamp          AS created_datetime,
    "receiveddatetime"::timestamp         AS received_datetime,
    "gnssposition" ->> 'Latitude'         AS latitude,
    "gnssposition" ->> 'Longitude'        AS longitude,
    ("gnssposition" ->> 'Heading')::int   AS heading,
    ("gnssposition" ->> 'Altitude')::int  AS altitude,
    ("gnssposition" ->> 'Speed')::float   AS gnss_speed,
    ("gnssposition" ->> 'PositionDateTime')::timestamp AS position_datetime,
    "wheelbasedspeed"::float              AS wheel_based_speed,
    "tachographspeed"::float              AS tachograph_speed,
    dbt_scd_id,
    dbt_updated_at,
    dbt_valid_from,
    dbt_valid_to

FROM {{ ref('volvo_vehicle_position_snapshot') }}
WHERE dbt_valid_to IS NULL
