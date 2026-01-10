SELECT
    job_id::TEXT         AS job_id,
    company_id::TEXT     AS company_id,
    layerconfig_id::TEXT AS layer_config_id,
    layeroption_id::TEXT AS layer_option_id,
    dimension::TEXT      AS dimension_id,
    name::TEXT           AS dimension_name,
    text::TEXT           AS dimension_text,
    dbt_scd_id,
    dbt_updated_at,
    dbt_valid_from,
    dbt_valid_to
FROM {{ ref('jobylon_job_layers_snapshot') }}
WHERE dbt_valid_to IS NULL
