WITH job_application AS (
    SELECT
        application_id,
        job_id,
        status_id,
        created_at,
        modified_at
    FROM {{ ref('stg__jobylon_application') }}
),

job AS (
    SELECT
        job_id,
        title
    FROM {{ ref('stg__jobylon_job') }}
),

job_layer_details AS (
    SELECT
        job_id,
        dimension_name,
        dimension_text,
        layer_option_id
    FROM {{ ref('stg__jobylon_job_layers') }}
),

application_status_log AS (
    SELECT
        status_log_id,
        application_id,
        status,
        created_at
    FROM {{ ref('stg__jobylon_application_status_log') }}
),

application_status_detail AS (
    SELECT
        status_log.status_log_id AS application_status_log_id,
        application.application_id,
        application.job_id,
        application.created_at   AS application_created_date,
        application.modified_at  AS application_last_modified_date,
        job.title                AS job_name,
        status_log.status,
        status_log.created_at    AS status_created_at
    FROM job_application AS application
    LEFT JOIN job ON application.job_id = job.job_id
    INNER JOIN application_status_log AS status_log
        ON application.application_id = status_log.application_id
),

division_mapping AS (
    SELECT
        division.id,
        division.division_id,
        division.tenant_id,
        division.data_source_id,
        division.ext_division_id
    FROM {{ ref('division_data_source_seed') }} AS division
    LEFT JOIN {{ ref('data_source_seed') }} AS data_source
        ON division.data_source_id = data_source.id
    WHERE data_source.name = 'Jobylon'
),

division_detail AS (
    SELECT
        division_id,
        division_name,
        company_id,
        company_name,
        tenant_id,
        tenant_name
    FROM {{ ref('int__department_denormalized') }}
    GROUP BY division_id, division_name, company_id, company_name, tenant_id, tenant_name
),

job_division AS (
    SELECT
        job_layer_details.job_id,
        job_layer_details.layer_option_id AS ext_division_id,
        division_mapping.division_id,
        division_detail.division_name,
        division_detail.company_id,
        division_detail.company_name,
        division_detail.tenant_id,
        division_detail.tenant_name
    FROM job_layer_details
    LEFT JOIN division_mapping
        ON job_layer_details.layer_option_id = division_mapping.ext_division_id
    LEFT JOIN division_detail
        ON division_mapping.division_id = division_detail.division_id
    WHERE job_layer_details.dimension_name = 'Division'
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['application_status_detail.application_id', 'application_status_detail.status']) }} AS application_status_id,
    application_status_detail.application_status_log_id,
    application_status_detail.application_id,
    application_status_detail.job_id,
    application_status_detail.job_name,
    application_status_detail.application_created_date,
    application_status_detail.application_last_modified_date,
    application_status_detail.status,
    application_status_detail.status_created_at,
    job_division.division_id,
    job_division.division_name,
    job_division.company_id,
    job_division.company_name,
    job_division.tenant_id,
    job_division.tenant_name
FROM application_status_detail
LEFT JOIN job_division
    ON application_status_detail.job_id = job_division.job_id
ORDER BY
    application_status_detail.application_id, application_status_detail.status_created_at
