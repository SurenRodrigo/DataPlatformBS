WITH ebit AS (
    SELECT
        company_name,
        division_name,
        department_name,
        project_identifier,
        period_date,
        month,
        month_name,
        year,
        period_value,
        acc_ebit,
        ytd_ebit,
        period_ebit,
        acc_revenue,
        period_revenue,
        ytd_revenue
    FROM {{ ref('fct_nrc_ebit') }}
),

project_cost AS (
    SELECT
        company_name,
        division_name,
        department_name,
        project_identifier,
        period_date,
        period_value,
        month_name,
        period AS month,
        year,
        acc_cost,
        (ytd_income - ytd_contribution) AS ytd_cost,
        (period_income - period_contribution) AS period_cost
    FROM {{ ref('project_portfolio') }}
)

SELECT
    ebit.*,
    project_cost.acc_cost,
    project_cost.ytd_cost,
    project_cost.period_cost
FROM ebit
FULL OUTER JOIN project_cost
    ON ebit.company_name = project_cost.company_name
    AND ebit.division_name = project_cost.division_name
    AND ebit.department_name = project_cost.department_name
    AND ebit.project_identifier = project_cost.project_identifier
    AND ebit.period_value = project_cost.period_value
