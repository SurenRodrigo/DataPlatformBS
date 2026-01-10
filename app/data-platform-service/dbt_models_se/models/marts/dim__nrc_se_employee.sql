SELECT
    {{ dbt_utils.generate_surrogate_key(['employee_no']) }} as employee_sk,
    employee_no,
    full_name,
    login,
    primary_group,
    job_role,
    employment_type,
    name,
    start_date,
    end_date,
    is_active,
    language,
    project_no
FROM {{ ref('stg__excel_employee') }}
