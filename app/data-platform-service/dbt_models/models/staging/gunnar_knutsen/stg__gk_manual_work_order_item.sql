SELECT
    "Department"                  AS department,
    "RoadNumber"                  AS road_no,
    TO_DATE("Date", 'DD.MM.YYYY') AS date,
    "Time"                        AS time,
    "Vehicle"                     AS vehicle,
    "Customer"                    AS customer,
    "Project Id"                  AS project_id,
    "Project"                     AS project_name,
    "Item"                        AS item,
    "Item Id"                     AS item_id,
    "PcsPerCubicMeter"            AS "pcs/m³",
    "Weight"                      AS weight,
    "Total Sum"                   AS sum_total,
    dbt_scd_id,
    dbt_updated_at,
    dbt_valid_from,
    dbt_valid_to
FROM {{ ref('manual_work_order_item_snapshot') }}
WHERE dbt_valid_to IS NULL
