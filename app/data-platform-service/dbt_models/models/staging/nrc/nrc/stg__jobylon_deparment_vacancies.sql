SELECT
    id AS department_vacancy_id,
    title,
    descr AS description,
    locations,
    "to_date"::DATE AS last_application_date,
    dt_created::DATE AS created_date,
    "urls"->>'ad' AS url,
    "company"->>'name' AS company_name,
    "contact"->>'email' AS contact_email,
    employment_type,
    dbt_valid_from,
    dbt_valid_to
FROM {{ ref('jobylon_job_posting_snapshot') }}