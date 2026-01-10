-- Volvo vehicle resources
WITH volvo_resources AS (
    SELECT
        resource_id AS vin,
        customer_vehicle_name AS vehicle_name,
        'Volvo' AS data_source_name
    FROM {{ ref('int__gk_vehicle_resources') }}
    WHERE data_source_name = 'Volvo'
),

-- Scania vehicle resources
scania_resources AS (
    SELECT
        resource_id AS vin,
        customer_vehicle_name AS vehicle_name,
        'Scania' AS data_source_name
    FROM {{ ref('int__gk_vehicle_resources') }}
    WHERE data_source_name = 'Scania'
),

-- Latest Volvo positions
volvo_latest_positions AS (
    SELECT
        vin,
        position_datetime,
        latitude,
        longitude,
        gnss_speed AS speed,
        heading,
        ROW_NUMBER() OVER (PARTITION BY vin ORDER BY position_datetime DESC) AS rn
    FROM {{ ref('stg__volvo_vehicle_position') }}
    WHERE latitude IS NOT NULL 
      AND longitude IS NOT NULL
      AND position_datetime IS NOT NULL
),

-- Latest Scania positions
scania_latest_positions AS (
    SELECT
        vin,
        position_datetime,
        latitude,
        longitude,
        wheel_based_speed AS speed,
        heading,
        ROW_NUMBER() OVER (PARTITION BY vin ORDER BY position_datetime DESC) AS rn
    FROM {{ ref('stg__scania_vehicle_positions') }}
    WHERE latitude IS NOT NULL
      AND longitude IS NOT NULL
      AND position_datetime IS NOT NULL
),

-- Combined latest positions
all_latest_positions AS (
    -- Volvo positions
    SELECT
        p.vin,
        v.vehicle_name,
        v.data_source_name,
        p.position_datetime,
        p.latitude,
        p.longitude,
        p.speed,
        p.heading
    FROM volvo_latest_positions p
    INNER JOIN volvo_resources v ON p.vin = v.vin
    WHERE p.rn = 1
    
    UNION ALL
    
    -- Scania positions
    SELECT
        p.vin,
        v.vehicle_name,
        v.data_source_name,
        p.position_datetime,
        p.latitude,
        p.longitude,
        p.speed,
        p.heading
    FROM scania_latest_positions p
    INNER JOIN scania_resources v ON p.vin = v.vin
    WHERE p.rn = 1
)

-- Final result with all vehicles
SELECT
    vin AS resource_id,
    vehicle_name,
    data_source_name,
    position_datetime,
    latitude,
    longitude,
    speed,
    heading
FROM all_latest_positions
ORDER BY position_datetime DESC
