WITH external_inputs AS (
    SELECT
        project_external_input_id,
        tenant_id,
        project_id,
        organizational_unit_id,
        external_input_type_id,
        input_date_time,
        "month",
        "year",
        "value",
        is_active,
        last_modified
    FROM {{ ref('project_external_inputs')}}
)

SELECT
    organizational_unit_id AS department_id,
    year,
    month,
    SUM(CASE WHEN external_input_type_id = 1 THEN CAST("value" AS NUMERIC) ELSE 0 END) AS rented_workers_hours_count,
    SUM(CASE WHEN external_input_type_id = 2 THEN CAST("value" AS NUMERIC) ELSE 0 END) AS subcontractor_hours_count,
    SUM(CASE WHEN external_input_type_id = 7 THEN CAST("value" AS NUMERIC) ELSE 0 END) AS rented_personal_count,
    last_modified
FROM external_inputs
WHERE external_input_type_id IN (1, 2, 3)
GROUP BY organizational_unit_id, year, month, last_modified
ORDER BY organizational_unit_id