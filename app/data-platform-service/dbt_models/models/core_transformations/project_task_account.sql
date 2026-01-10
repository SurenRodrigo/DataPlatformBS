SELECT
    project_task_account_sk,
    tenant_id,
    tenant_name,
    project_id,
    project_task_sk,
    ext_project_task_id,
    project_task_name,
    project_name,
    project_identifier,
    company_id,
    company_name,
    division_id,
    division_name,
    department_id,
    department_name,
    year,
    period,
    period_value,
    CASE
        WHEN
            period BETWEEN 1 AND 12
            THEN TO_DATE(year || LPAD(period::text, 2, '0') || '01', 'YYYYMMDD')
    END AS period_date,
    amount_period,
    amount_acc,
    amount_ytd,
    amount_type,
    data_source_id,
    data_source_name
FROM {{ ref('int__nrc_project_task_account') }}
