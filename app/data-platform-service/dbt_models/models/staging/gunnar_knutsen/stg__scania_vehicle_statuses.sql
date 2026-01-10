SELECT
    "vin" AS vin,

    "driver1id"::jsonb -> 'oemDriverIdentification' ->> 'idType' AS oem_driver_id_type,
    "driver1id"::jsonb -> 'oemDriverIdentification' ->> 'oemDriverIdentification' AS oem_driver_id,
    "driver1id"::jsonb -> 'tachoDriverIdentification' ->> 'driverIdentification' AS tacho_driver_id,
    "driver1id"::jsonb -> 'tachoDriverIdentification' ->> 'cardIssuingMemberState' AS tacho_driver_card_issuer,

    "doorstatus" AS door_status,

    "uptimedata"::jsonb ->> 'tellTaleInfo' AS tell_tale_info,
    "uptimedata"::jsonb ->> 'alternatorInfo' AS alternator_info,
    "uptimedata"::jsonb ->> 'serviceDistance' AS service_distance,
    "uptimedata"::jsonb ->> 'engineCoolantTemperature' AS engine_coolant_temperature,
    "uptimedata"::jsonb ->> 'serviceBrakeAirPressureCircuit1' AS brake_air_pressure_circuit_1,
    "uptimedata"::jsonb ->> 'serviceBrakeAirPressureCircuit2' AS brake_air_pressure_circuit_2,

    "triggertype"::jsonb ->> 'context' AS trigger_type_context,
    "triggertype"::jsonb ->> 'triggerType' AS trigger_type,

    "snapshotdata"::jsonb ->> 'fuelLevel1' AS fuelLevel,
    "snapshotdata"::jsonb ->> 'gnssPosition' AS gnss_position,
    "snapshotdata"::jsonb -> 'gnssPosition' ->> 'latitude' AS latitude,
    "snapshotdata"::jsonb -> 'gnssPosition' ->> 'longitude' AS longitude,
    "snapshotdata"::jsonb -> 'gnssPosition' ->> 'positionDateTime' AS position_datetime,
    "snapshotdata"::jsonb -> 'gnssPosition' ->> 'heading' AS heading,
  
    "snapshotdata"::jsonb ->> 'positionDateTime' AS statusDate,
    "snapshotdata"::jsonb ->> 'tachographSpeed' AS tachograph_speed,
    "snapshotdata"::jsonb ->> 'wheelBasedSpeed' AS wheel_based_speed,
    "snapshotdata"::jsonb ->> 'catalystFuelLevel' AS catalyst_fuel_level,
    "snapshotdata"::jsonb ->> 'driver1WorkingState' AS driver1_working_state,
    "snapshotdata"::jsonb ->> 'driver2WorkingState' AS driver2_working_state,
    "snapshotdata"::jsonb ->> 'ambientAirTemperature' AS ambient_air_temperature,
    "snapshotdata"::jsonb ->> 'estimatedDistanceToEmpty' AS estimated_distance_to_empty,

    "status2ofdoors" AS status_2_of_doors,

    "accumulateddata"::jsonb ->> 'ptoActiveClass' AS pto_active_class,
    "accumulateddata"::jsonb ->> 'currentGearClass' AS current_gear_class,
    "accumulateddata"::jsonb ->> 'engineSpeedClass' AS engine_speed_class,
    "accumulateddata"::jsonb ->> 'accelerationClass' AS acceleration_class,
    "accumulateddata"::jsonb ->> 'engineTorqueClass' AS engine_torque_class,
    "accumulateddata"::jsonb ->> 'selectedGearClass' AS selected_gear_class,
    "accumulateddata"::jsonb ->> 'vehicleSpeedClass' AS vehicle_speed_class,
    "accumulateddata"::jsonb ->> 'retarderTorqueClass' AS retarder_torque_class,
    "accumulateddata"::jsonb ->> 'highAccelerationClass' AS high_acceleration_class,
    "accumulateddata"::jsonb ->> 'drivingWithoutTorqueClass' AS driving_without_torque_class,
    "accumulateddata"::jsonb ->> 'accelerationDuringBrakeClass' AS acceleration_during_brake_class,
    "accumulateddata"::jsonb ->> 'accelerationPedalPositionClass' AS acceleration_pedal_position_class,
    "accumulateddata"::jsonb ->> 'engineTorqueAtCurrentSpeedClass' AS engine_torque_at_current_speed_class,

    "createddatetime"::timestamp   AS created_date_time,
    "receiveddatetime"::timestamp   AS received_date_time,
    "totalenginehours" AS engineTotalHours,
    "enginetotalfuelused" AS engine_total_fuel_used,
    "hrtotalvehicledistance" AS odometer,
    "totalelectricenergyused" AS total_electric_energy_used,
    "totalelectricmotorhours" AS total_electric_motor_hours,
    "grosscombinationvehicleweight" AS gross_combination_vehicle_weight,

    -- DBT Metadata
    dbt_scd_id,
    dbt_updated_at,
    dbt_valid_from,
    dbt_valid_to

FROM {{ ref('scania_vehicle_statuses_snapshot') }}
WHERE dbt_valid_to IS NULL
