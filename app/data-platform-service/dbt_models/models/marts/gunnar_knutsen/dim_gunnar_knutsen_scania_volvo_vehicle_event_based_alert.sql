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

-- Volvo position events
volvo_position_events AS (
    SELECT
        CONCAT(vin, '_', created_datetime::text) AS event_id,
        created_datetime AS event_datetime,
        trigger_type AS event_type,
        vin,
        latitude::float AS latitude,
        longitude::float AS longitude,
        position_datetime,
        'POSITION' AS event_category,
        'Volvo' AS data_source_name
    FROM {{ ref('stg__volvo_vehicle_position') }}
    WHERE trigger_type IS NOT NULL
      AND latitude IS NOT NULL 
      AND longitude IS NOT NULL
),

-- Volvo status events
volvo_status_events AS (
    SELECT
        CONCAT(vin, '_', created_at::text) AS event_id,
        created_at AS event_datetime,
        trigger_type AS event_type,
        vin,
        latitude::float AS latitude,
        longitude::float AS longitude,
        created_at AS position_datetime,
        'STATUS' AS event_category,
        'Volvo' AS data_source_name
    FROM {{ ref('stg__volvo_vehicle_status') }}
    WHERE trigger_type IS NOT NULL
),

-- Scania trigger events
scania_trigger_events AS (
    SELECT
        CONCAT(vin, '_', created_date_time::text) AS event_id,
        created_date_time AS event_datetime,
        trigger_type AS event_type,
        vin,
        latitude::float AS latitude,
        longitude::float AS longitude,
        position_datetime::timestamp AS position_datetime,
        'TRIGGER' AS event_category,
        'Scania' AS data_source_name
    FROM {{ ref('stg__scania_vehicle_statuses') }}
    WHERE trigger_type IS NOT NULL
      AND gnss_position IS NOT NULL
),

-- Scania door events
scania_door_events AS (
    SELECT
        CONCAT(vin, '_', created_date_time::text) AS event_id,
        created_date_time AS event_datetime,
        CONCAT(COALESCE(door_status::text, 'UNKNOWN')) AS event_type,
        vin,
        latitude::float AS latitude,
        longitude::float AS longitude,
        position_datetime::timestamp AS position_datetime,
        'DOOR' AS event_category,
        'Scania' AS data_source_name
    FROM {{ ref('stg__scania_vehicle_statuses') }}
    WHERE door_status IS NOT NULL
      AND gnss_position IS NOT NULL
),

-- Scania tell tale events
scania_tell_tale_events AS (
    SELECT
        CONCAT(vin, '_', created_date_time::text) AS event_id,
        created_date_time AS event_datetime,
        'TELL_TALE' AS event_type,
        vin,
        latitude::float AS latitude,
        longitude::float AS longitude,
        position_datetime::timestamp AS position_datetime,
        'ALERT' AS event_category,
        'Scania' AS data_source_name
    FROM {{ ref('stg__scania_vehicle_statuses') }}
    WHERE tell_tale_info IS NOT NULL
      AND tell_tale_info != ''
      AND gnss_position IS NOT NULL
),

-- All Volvo events
all_volvo_events AS (
    SELECT * FROM volvo_position_events
    UNION ALL
    SELECT * FROM volvo_status_events
),

-- All Scania events
all_scania_events AS (
    SELECT * FROM scania_trigger_events
    UNION ALL
    SELECT * FROM scania_door_events
    UNION ALL
    SELECT * FROM scania_tell_tale_events
),

-- Combined all events
all_events AS (
    SELECT * FROM all_volvo_events
    UNION ALL
    SELECT * FROM all_scania_events
)

-- Final result with all vehicle events
SELECT
    e.event_id,
    e.vin AS resource_id,
    v.vehicle_name,
    e.data_source_name,
    e.event_datetime,
    e.event_type,
    e.event_category,
    e.latitude,
    e.longitude,
    e.position_datetime
FROM all_events e
INNER JOIN (
    SELECT * FROM volvo_resources
    UNION ALL
    SELECT * FROM scania_resources
) v ON e.vin = v.vin
WHERE e.event_type IS NOT NULL
ORDER BY e.event_datetime DESC
