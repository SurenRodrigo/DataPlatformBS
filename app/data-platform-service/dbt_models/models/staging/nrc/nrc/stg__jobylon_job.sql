SELECT
    id::VARCHAR                              AS job_id,
    title::TEXT                              AS title,
    status::TEXT                             AS status,
    to_date::DATE                            AS to_date,
    from_date::DATE                          AS from_date,
    COALESCE(is_hidden = 1, FALSE)           AS is_hidden,
    job_owner::TEXT                          AS job_owner,
    company_id::TEXT                         AS company_id,
    dt_created::TIMESTAMP                    AS created_at,
    dt_modified::TIMESTAMP                   AS modified_at,
    COALESCE(is_internal = 1, FALSE)         AS is_internal,
    COALESCE(is_template = 1, FALSE)         AS is_template,
    COALESCE(is_confidential = 1, FALSE)     AS is_confidential,
    internal_reference::TEXT                 AS internal_reference,
    dbt_scd_id,
    dbt_updated_at,
    dbt_valid_from,
    dbt_valid_to
FROM {{ ref('jobylon_job_snapshot') }}
WHERE dbt_valid_to IS NULL
