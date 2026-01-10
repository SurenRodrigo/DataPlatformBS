SELECT
    id                         AS project_id,
    NULLIF(name, '')           AS project_name,
    (NULLIF(projectnumber, '')::NUMERIC)::INT AS project_number,
    organizationid             AS ext_organization_id,
    isFinished                 AS is_finished,
    dbt_scd_id,
    dbt_updated_at,
    dbt_valid_from,
    dbt_valid_to
FROM {{ ref('endringer_project_snapshot') }}
WHERE dbt_valid_to IS NULL
