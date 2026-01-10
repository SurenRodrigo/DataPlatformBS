-- Monthly data
SELECT
    year AS year,
    created_month_name AS month_name,
    company_name,
    division_name,
    department_name,
    project_identifier,
    status,
    variation_date,
    LPAD(EXTRACT(MONTH FROM variation_date)::text, 2, '0') || '-' || TO_CHAR(variation_date, 'Mon') as period,
    variation_type_name,
    SUM(total_amount) as total_amount,
    SUM(accepted_amount) as accepted_amount,
    SUM(declined_amount) as declined_amount,
    SUM(not_handled_amount) as not_handled_amount
FROM {{ ref('fct_nrc_project_variations') }}
WHERE variation_date IS NOT NULL
GROUP BY 
    year,
    created_month_name,
    company_name, 
    division_name, 
    department_name, 
    project_identifier, 
    status, 
    variation_type_name, 
    variation_date

UNION ALL

-- YTD Total bar
SELECT
    year AS year,
    NULL AS month_name,
    company_name,
    division_name,
    department_name,
    project_identifier,
    status,
    MAX(variation_date) as variation_date,  -- Moved to correct position
    'YTD Total' as period, 
    variation_type_name,
    SUM(total_amount) as total_amount,
    SUM(accepted_amount) as accepted_amount,
    SUM(declined_amount) as declined_amount,
    SUM(not_handled_amount) as not_handled_amount
FROM {{ ref('fct_nrc_project_variations') }}
WHERE variation_date IS NOT NULL
GROUP BY 
    year,
    company_name, 
    division_name, 
    department_name, 
    project_identifier, 
    status, 
    variation_type_name, 
    variation_date
ORDER BY period, variation_type_name