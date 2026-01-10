SELECT
    {{ dbt_utils.generate_surrogate_key(['application_status_id', 'division_id']) }} AS application_status_sk,
    application_status_id,
    application_status_log_id,
    application_id,
    job_id,
    job_name,
    application_created_date,
    application_last_modified_date,
    status,
    status_created_at,
    division_id,
    division_name,
    company_id,
    company_name,
    tenant_id,
    tenant_name
FROM {{ ref('int__nrc_job_applicant_status') }}
WHERE company_id IS NOT NULL AND division_id IS NOT NULL
