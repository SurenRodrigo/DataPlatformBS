WITH time_types_base AS (
    SELECT
        absence_type_id   AS time_type_id,
        absence_type_name AS time_types
    FROM {{ ref('stg__admmit_hours') }}
    GROUP BY absence_type_id, absence_type_name

    UNION ALL

    SELECT
        1,
        'Ordinary Hours'

    UNION ALL

    SELECT
        2,
        'Overtime Hours'
)

SELECT
    time_type_id,
    time_types,
    'Admmit' AS data_source_id
FROM time_types_base
WHERE time_type_id IS NOT NULL
