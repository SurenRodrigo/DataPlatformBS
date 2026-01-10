WITH all_resources AS (
    SELECT
        resource_id AS vin,
        customer_vehicle_name AS vehicle_name,
        data_source_name,
        brand,
        model
    FROM {{ ref('int__gk_vehicle_resources') }}
    WHERE data_source_name IN ('Volvo', 'Scania')
),

scania_position_data AS (
    SELECT
        vin,
        DATE(created_datetime) AS date,
        latitude::float AS latitude,
        longitude::float AS longitude,
        speedgnss AS gnss_speed,
        triggertype AS trigger_type,
        position_datetime,
        created_datetime,
        ROW_NUMBER() OVER (PARTITION BY vin, DATE(created_datetime) ORDER BY created_datetime DESC) AS rn_last
    FROM {{ ref('stg__scania_vehicle_positions') }}
    WHERE vin IN (SELECT vin FROM all_resources WHERE data_source_name = 'Scania')
),

volvo_position_data AS (
    SELECT
        vin,
        DATE(created_datetime) AS date,
        latitude::float AS latitude,
        longitude::float AS longitude,
        gnss_speed,
        trigger_type,
        position_datetime,
        created_datetime,
        ROW_NUMBER() OVER (PARTITION BY vin, DATE(created_datetime) ORDER BY created_datetime DESC) AS rn_last
    FROM {{ ref('stg__volvo_vehicle_position') }}
    WHERE vin IN (SELECT vin FROM all_resources WHERE data_source_name = 'Volvo')
),

-- Unified position data
position_data AS (
    SELECT
        vin, date, latitude, longitude, gnss_speed, trigger_type, position_datetime, 
        created_datetime AS created_date_time, rn_last
    FROM scania_position_data
    
    UNION ALL
    
    SELECT
        vin, date, latitude, longitude, gnss_speed, trigger_type, position_datetime,
        created_datetime AS created_date_time, rn_last
    FROM volvo_position_data
),

-- Vehicle status data from Scania for odometer and engine metrics
scania_status_data AS (
    SELECT
        vin,
        DATE(created_date_time) AS date,
        created_date_time,
        odometer AS hr_total_vehicle_distance,
        engineTotalHours AS total_engine_hours,
        (fuelLevel)::numeric AS fuel_level_percent,
        trigger_type,
        (wheel_based_speed)::float AS wheel_based_speed,
        (tachograph_speed)::float AS tachograph_speed,
        NULL::float AS engine_speed
    FROM {{ ref('stg__scania_vehicle_statuses') }}
    WHERE odometer IS NOT NULL
      AND odometer > 0
      AND vin IN (SELECT vin FROM all_resources WHERE data_source_name = 'Scania')
),

-- Vehicle status data from Volvo for odometer and engine metrics
volvo_status_data AS (
    SELECT
        vin,
        DATE(created_at) AS date,
        created_at AS created_date_time,
        total_distance AS hr_total_vehicle_distance,
        total_engine_hours,
        NULL::numeric AS fuel_level_percent,
        trigger_type,
        (wheel_based_speed)::float AS wheel_based_speed,
        (tachograph_speed)::float AS tachograph_speed,
        (engine_speed)::float AS engine_speed
    FROM {{ ref('stg__volvo_vehicle_status') }}
    WHERE total_distance IS NOT NULL
      AND total_distance > 0
),

-- Unified status data
status_data AS (
    SELECT
        vin, date, created_date_time, hr_total_vehicle_distance, 
        total_engine_hours, fuel_level_percent, trigger_type,
        wheel_based_speed, tachograph_speed, engine_speed
    FROM scania_status_data
    
    UNION ALL
    
    SELECT
        vin, date, created_date_time, hr_total_vehicle_distance,
        total_engine_hours, fuel_level_percent, trigger_type,
        wheel_based_speed, tachograph_speed, engine_speed
    FROM volvo_status_data
),

-- Fixed idle time calculation - get next timestamp from ALL status data
idle_time_calc AS (
    SELECT
        idle_start.vin,
        idle_start.date,
        idle_start.created_date_time AS idle_start_time,
        MIN(next_status.created_date_time) AS next_status_time
    FROM (
        -- Get idle start points
        SELECT
            vin,
            DATE(created_date_time) AS date,
            created_date_time
        FROM status_data
        WHERE trigger_type = 'IGNITION_ON'
          AND COALESCE(wheel_based_speed, 0) = 0 
          AND COALESCE(tachograph_speed, 0) = 0
    ) idle_start
    LEFT JOIN (
        SELECT
            vin,
            DATE(created_date_time) AS date,
            created_date_time
        FROM status_data
    ) next_status ON idle_start.vin = next_status.vin 
                   AND idle_start.date = next_status.date
                   AND next_status.created_date_time > idle_start.created_date_time
    GROUP BY idle_start.vin, idle_start.date, idle_start.created_date_time
),

idle_time_agg AS (
    SELECT
        vin,
        date,
        SUM(
            EXTRACT(EPOCH FROM (next_status_time - idle_start_time))/60
        ) AS idle_time_minutes
    FROM idle_time_calc
    WHERE next_status_time IS NOT NULL
      AND next_status_time > idle_start_time
      AND EXTRACT(EPOCH FROM (next_status_time - idle_start_time))/60 >= 1
    GROUP BY vin, date
),

-- Proper odometer aggregation with ordered readings
odometer_agg AS (
    SELECT
        vin,
        date,
        MIN(CASE WHEN rn_first = 1 THEN hr_total_vehicle_distance END) AS odometer_start,
        MAX(CASE WHEN rn_last = 1 THEN hr_total_vehicle_distance END) AS odometer_end,
        COUNT(*) AS readings_count,
        MIN(hr_total_vehicle_distance) AS min_odometer,
        MAX(hr_total_vehicle_distance) AS max_odometer
    FROM (
        SELECT 
            vin,
            date,
            hr_total_vehicle_distance,
            ROW_NUMBER() OVER (PARTITION BY vin, date ORDER BY created_date_time ASC) as rn_first,
            ROW_NUMBER() OVER (PARTITION BY vin, date ORDER BY created_date_time DESC) as rn_last
        FROM status_data
        WHERE hr_total_vehicle_distance IS NOT NULL
          AND hr_total_vehicle_distance > 0
    ) ordered_readings
    GROUP BY vin, date
),

-- Aggregated status metrics per vehicle per day
status_agg AS (
    SELECT
        vin,
        date,
        AVG(NULLIF(fuel_level_percent, 0)) AS avg_fuel_level_percent,
        MIN(total_engine_hours) AS start_engine_hours,
        MAX(total_engine_hours) AS end_engine_hours,
        (MAX(total_engine_hours) - MIN(total_engine_hours)) AS daily_engine_hours
    FROM status_data
    WHERE total_engine_hours IS NOT NULL
    GROUP BY vin, date
),

agg_position AS (
    SELECT
        vin,
        date,
        AVG(NULLIF(gnss_speed, 0)) AS avg_speed_kmh,
        MAX(gnss_speed) AS max_speed_kmh,
        COUNT(CASE WHEN gnss_speed > 0 THEN 1 END) AS moving_readings_count,
        COUNT(*) AS total_readings_count
    FROM position_data
    WHERE gnss_speed IS NOT NULL
    GROUP BY vin, date
),

last_position AS (
    SELECT
        vin,
        date,
        latitude AS lat,
        longitude AS lon
    FROM position_data
    WHERE rn_last = 1
)

SELECT
    o.vin AS resourceId,
    v.vehicle_name AS vehicleName,
    v.brand AS vehicleBrand,
    v.model AS vehicleModel,
    o.date,
    o.odometer_start AS odometerStart,
    o.odometer_end AS odometerEnd,
    CASE 
        WHEN o.odometer_end IS NOT NULL AND o.odometer_start IS NOT NULL 
        THEN ROUND((o.odometer_end - o.odometer_start)::numeric / 1000.0, 2)
        ELSE 0 
    END AS dailyDistanceKm,
    COALESCE(s.avg_fuel_level_percent, 0) AS fuelLevelPercent,
    COALESCE(s.daily_engine_hours, 0) AS engineHours,
    COALESCE(i.idle_time_minutes, 0) AS idleTimeMinutes,
    a.avg_speed_kmh AS avgSpeedKmH,
    l.lat,
    l.lon
FROM odometer_agg o
LEFT JOIN status_agg s ON o.vin = s.vin AND o.date = s.date
LEFT JOIN agg_position a ON o.vin = a.vin AND o.date = a.date
LEFT JOIN last_position l ON o.vin = l.vin AND o.date = l.date
LEFT JOIN all_resources v ON o.vin = v.vin
LEFT JOIN idle_time_agg i ON o.vin = i.vin AND o.date = i.date
WHERE o.vin IS NOT NULL
