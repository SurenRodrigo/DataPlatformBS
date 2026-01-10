WITH project_portfolio AS (
    SELECT
        project_portfolio_sk,
        tenant_id,
        ext_company_id,
        company_id,
        company_name,
        division_id,
        division_name,
        department_id,
        department_name,
        project_id,
        project_name,
        project_number,
        project_identifier,
        project_type_code,
        status,
        ytd_estimate,
        acc_ebit,
        acc_income,
        or_currency,
        project_full_name,
        year_investment,
        ytd_ebit,
        ytd_income,
        ext_division_id,
        or_currency1,
        or_currency2,
        acc_contribution,
        period_investment,
        ytd_contribution,
        or_currency_res,
        order_stock,
        period_ebit,
        period_income,
        project_type_text,
        or_currency_res1,
        or_currency_res2,
        acc_estimate_margin,
        period_contribution,
        ytd_estimate_margin,
        year,
        period,
        month_name,
        period_value,
        acc_estimate,
        acc_cost,
        invested_capital,
        period_estimate,
        CASE 
            WHEN period BETWEEN 1 AND 12 THEN TO_DATE(year || LPAD(period::text, 2, '0') || '01', 'YYYYMMDD')
            ELSE NULL
        END AS period_date,
        data_source_id
    FROM {{ ref('int__nrc_project_portfolio')}}
) 

SELECT *
FROM project_portfolio