WITH fact_ebit AS (
    SELECT
        company_id,
        company_name,
        division_id,
        division_name,
        department_id,
        department_name,
        year,
        month,
        month_name,
        period_value,
        period_date,
        project_identifier,
        acc_ebit,
        ytd_ebit,
        period_ebit,
        acc_revenue,
        period_revenue,
        ytd_revenue,
        data_source_name
    FROM {{ ref('fct_nrc_ebit') }}
),

fact_liquidity AS (
    SELECT
        company_id,
        company_name,
        division_id,
        division_name,
        department_id,
        department_name,
        project_number,
        project_identifier,
        period_value,
        period,
        period_date,
        year,
        month,
        month_name,
        vo,
        amount_acc,
        amount_acc - LAG(amount_acc) OVER (
            PARTITION BY company_id, division_id, department_id, project_number, project_identifier
            ORDER BY period_value
        ) AS period_liquidity
    FROM {{ ref('fct_nrc_liquidity_interim') }}
    WHERE org_hierarchy_level = 'project'
    AND amount_type = 'liquidity'
)

SELECT 
    -- Common fields
    COALESCE(fact_ebit.company_id, fact_liquidity.company_id) AS company_id,
    COALESCE(fact_ebit.company_name, fact_liquidity.company_name) AS company_name,
    COALESCE(fact_ebit.division_id, fact_liquidity.division_id) AS division_id,
    COALESCE(fact_ebit.division_name, fact_liquidity.division_name) AS division_name,
    COALESCE(fact_ebit.department_id, fact_liquidity.department_id) AS department_id,
    COALESCE(fact_ebit.department_name, fact_liquidity.department_name) AS department_name,
    COALESCE(fact_ebit.project_identifier, fact_liquidity.project_identifier) AS project_identifier,
    fact_liquidity.project_number,
    COALESCE(fact_ebit.year, fact_liquidity.year) AS year,
    COALESCE(fact_ebit.month, fact_liquidity.month) AS month,
    COALESCE(fact_ebit.month_name, fact_liquidity.month_name) AS month_name,
    COALESCE(fact_ebit.period_date, fact_liquidity.period_date) AS period_date,
    COALESCE(fact_ebit.period_value, fact_liquidity.period_value) AS period_value,
    
    -- EBIT fields
    fact_ebit.acc_ebit,
    fact_ebit.ytd_ebit,
    fact_ebit.period_ebit,
    fact_ebit.acc_revenue,
    fact_ebit.period_revenue,
    fact_ebit.ytd_revenue,
    -- Liquidity fields
    fact_liquidity.vo,
    fact_liquidity.amount_acc AS acc_liquidity,
    fact_liquidity.period_liquidity AS period_liquidity
    
FROM fact_ebit
FULL OUTER JOIN fact_liquidity
    ON fact_ebit.company_id = fact_liquidity.company_id
    AND fact_ebit.division_id = fact_liquidity.division_id
    AND fact_ebit.department_id = fact_liquidity.department_id
    AND fact_ebit.project_identifier = fact_liquidity.project_identifier
    AND fact_ebit.period_value = fact_liquidity.period_value