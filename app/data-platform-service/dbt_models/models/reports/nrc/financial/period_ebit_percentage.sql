WITH project_portfolio AS (
    SELECT
        company_name,
        division_name,
        department_name,
        project_number,
        project_identifier,
        project_type_code,
        status,
        project_full_name,
        period_ebit,
        period_income,
        project_type_text,
        year,
        period,
        month_name,
        period_date,
        period_value,
        period_estimate,
        COALESCE(ROUND(((period_ebit::numeric) / NULLIF(period_estimate::numeric, 0)) * 100, 2), 0) AS ebit_percentage
    FROM {{ ref('project_portfolio')}}
)

SELECT *
FROM project_portfolio


