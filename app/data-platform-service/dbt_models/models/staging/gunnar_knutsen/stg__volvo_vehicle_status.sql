SELECT
    vin,
    "triggertype" ->> 'TriggerType'                        AS trigger_type,
    "triggertype" ->> 'Context'                            AS trigger_context,
    "triggertype" -> 'DriverId' -> 'TachoDriverIdentification' ->> 'DriverAuthenticationEquipment'      AS driver_auth_equipment,
    "triggertype" -> 'DriverId' -> 'TachoDriverIdentification' ->> 'DriverIdentification'               AS driver_id,
    "triggertype" -> 'DriverId' -> 'TachoDriverIdentification' ->> 'CardIssuingMemberState'             AS driver_card_issuer,
    "createddatetime"::timestamp
        AS created_at,
    "receiveddatetime"::timestamp
        AS received_at,
    "hrtotalvehicledistance"
        AS total_distance,
    "enginetotalfuelused"
        AS engine_fuel_used,
    "grosscombinationvehicleweight"
        AS gross_weight,
    "snapshotdata"
    ->> 'GNSSPosition'                       AS gns_position,
    "snapshotdata"
    -> 'GNSSPosition' ->> 'Latitude'         AS latitude,
    "snapshotdata"
    -> 'GNSSPosition' ->> 'Longitude'        AS longitude,
    "snapshotdata"
    -> 'GNSSPosition' ->> 'PositionDateTime' AS position_datetime,
    "snapshotdata"
    -> 'GNSSPosition' ->> 'Heading'          AS heading,
    "snapshotdata"
    -> 'GNSSPosition' ->> 'Altitude'         AS altitude,
    "snapshotdata"
    -> 'GNSSPosition' ->> 'Speed'            AS speed,
    "snapshotdata"
    ->> 'WheelBasedSpeed'                    AS wheel_based_speed,
    "snapshotdata"
    ->> 'TachographSpeed'                    AS tachograph_speed,
    "snapshotdata"
    ->> 'EngineSpeed'                        AS engine_speed,
    "snapshotdata"
    ->> 'Driver1WorkingState'                AS driver1_working_state,
    "snapshotdata"
    ->> 'Driver2Id'                          AS driver2_id,
    "snapshotdata"
    ->> 'Driver2WorkingState'                AS driver2_working_state,
    "snapshotdata"
    ->> 'AmbientAirTemperature'              AS ambient_air_temperature,
    "snapshotdata"
    ->> 'FuelLevel1'                          AS fuelLevel,
    "driver1id"
    -> 'TachoDriverIdentification'
    ->> 'DriverAuthenticationEquipment'      AS driver1_auth_equipment,
    "driver1id"
    -> 'TachoDriverIdentification'
    ->> 'DriverIdentification'               AS driver1_id,
    "driver1id"
    -> 'TachoDriverIdentification'
    ->> 'CardIssuingMemberState'             AS driver1_card_issuer,
    "accumulateddata"
    ->> 'DurationWheelbaseSpeedOverZero'     AS duration_wheelbase_moving,
    "accumulateddata"
    ->> 'DurationWheelbaseSpeedZero'         AS duration_wheelbase_stopped,
    "accumulateddata"
    ->> 'DistanceCruiseControlActive'        AS cruise_distance,
    "accumulateddata"
    ->> 'DurationCruiseControlActive'        AS cruise_duration,
    "accumulateddata"
    ->> 'FuelConsumptionCruiseControlActive' AS cruise_fuel_used,
    "accumulateddata"
    ->> 'VehicleSpeedClass'                  AS vehicle_speed_class,
    "totalenginehours"                       AS total_engine_hours,

    dbt_scd_id,
    dbt_updated_at,
    dbt_valid_from,
    dbt_valid_to

FROM {{ ref('volvo_vehicle_status_snapshot') }}
WHERE dbt_valid_to IS NULL
