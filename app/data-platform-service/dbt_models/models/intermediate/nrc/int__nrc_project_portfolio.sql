WITH company_mapping AS (
    SELECT
        ext_company_id,
        company_guid,
        data_source_id,
        tenant_id
    FROM {{ ref('company_data_source_seed')}}
),

organizations AS (
    SELECT
        department_id,
        department_name,
        division_id,
        division_name,
        company_id,
        company_name,
        data_source_id,
        data_source_name
    FROM {{ ref('int__department_external_id_mapping') }}
)


SELECT 
    {{ dbt_utils.generate_surrogate_key(['project_portfolio.ext_company_id', 'project_portfolio.period', 'project_portfolio.project_number']) }} AS project_portfolio_sk,
    company.tenant_id,
    project_portfolio.ext_company_id,
    org.company_id,
    org.company_name,
    org.division_id,
    org.division_name,
    org.department_id,
    org.department_name,
    project.id AS project_id,
    LEFT(period, 4)::INT AS year,
    RIGHT(period, 2)::INT AS period,
    CASE 
        WHEN RIGHT(period, 2)::INT BETWEEN 1 AND 12 
            THEN TO_CHAR(TO_DATE(RIGHT(period, 2)::TEXT, 'MM'), 'MON')
        ELSE NULL
    END AS month_name,
    project_portfolio.period AS period_value,
    project.project_name,
    project_identifier,
    project_portfolio.project_number,
    project_portfolio.project_type_code,
    project_portfolio.project_full_name,
    CASE 
        WHEN status = 'Sperret' THEN 'Blocked'
        WHEN status = 'Parkert' THEN 'Parked'
        WHEN status = 'Aktiv' THEN 'Active'
        ELSE status
    END                  AS status,
    project_portfolio.ytd_estimate,
    project_portfolio.ytd_estimate - COALESCE(LAG(project_portfolio.ytd_estimate) OVER (
        PARTITION BY project_portfolio.ext_company_id, project_portfolio.project_number
        ORDER BY project_portfolio.period
    ), 0) AS period_estimate,
    project_portfolio.acc_ebit,
    project_portfolio.acc_income,
    project_portfolio.or_currency,
    project_portfolio.year_investment,
    project_portfolio.ytd_ebit,
    project_portfolio.ytd_income,
    project_portfolio.ext_division_id,
    project_portfolio.or_currency1,
    project_portfolio.or_currency2,
    project_portfolio.acc_contribution,
    project_portfolio.period_investment,
    project_portfolio.ytd_contribution,
    project_portfolio.or_currency_res,
    project_portfolio.order_stock,
    project_portfolio.period_ebit,
    project_portfolio.period_income,
    project_portfolio.project_type_text,
    project_portfolio.or_currency_res1,
    project_portfolio.or_currency_res2,
    project_portfolio.acc_estimate_margin,
    project_portfolio.period_contribution,
    project_portfolio.ytd_estimate_margin,
    project_portfolio.acc_estimate,
    project_portfolio.acc_cost,
    project_portfolio.invested_capital,
    company.data_source_id
FROM {{ ref('stg_unit4_project_portfolio') }} AS project_portfolio
LEFT JOIN {{ ref('company_data_source_seed') }} AS company
    ON company.ext_company_id = project_portfolio.ext_company_id::TEXT
LEFT JOIN {{ ref('project') }} AS project
    ON project.ext_project_id = project_portfolio.project_number
    AND project.company_id = company.company_guid
LEFT JOIN organizations AS org
    ON org.department_id = project.department_id
    AND org.data_source_id = company.data_source_id
    AND org.data_source_name = 'Unit4'
