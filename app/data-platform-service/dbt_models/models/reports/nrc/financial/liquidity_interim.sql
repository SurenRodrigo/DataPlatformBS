SELECT
    sk_id,
    org_hierarchy_level,
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
    amount_type,
    amount_acc,
    amount_year,
    CASE
        WHEN amount_year IS NOT NULL
            THEN
                amount_year - COALESCE(
                    LAG(amount_year) OVER (
                        PARTITION BY
                            company_id,
                            division_id,
                            department_id,
                            project_number,
                            vo,
                            amount_type,
                            year
                        ORDER BY month
                    ), 0
                )
    END AS period_spend,
    data_source_name
FROM {{ ref('fct_nrc_liquidity_interim') }}
