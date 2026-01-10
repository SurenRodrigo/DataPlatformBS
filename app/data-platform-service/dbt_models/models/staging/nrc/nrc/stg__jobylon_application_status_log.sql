SELECT
    id::VARCHAR                             AS status_log_id,
    application_id::TEXT                    AS application_id,
    status                                  AS status,
    dt_created::TIMESTAMP                   AS created_at,
    dbt_scd_id,
    dbt_updated_at,
    dbt_valid_from,
    dbt_valid_to
FROM {{ ref('jobylon_application_status_log_snapshot') }}
WHERE dbt_valid_to IS NULL
