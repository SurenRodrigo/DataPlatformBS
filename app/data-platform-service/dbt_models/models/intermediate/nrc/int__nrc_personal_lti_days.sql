SELECT
    year_occurred::TEXT                                   AS year,
    month_occurred                                        AS month,
    TO_CHAR(TO_DATE(month_occurred::TEXT, 'MM'), 'Month') AS month_name,
    TO_CHAR(
        TO_DATE(
            year_occurred::TEXT || '-' || LPAD(month_occurred::TEXT, 2, '0') || '-01',
            'YYYY-MM-DD'
        ),
        'Mon YYYY'
    )                                                     AS month_label,
    TO_DATE(
        CONCAT(year_occurred::TEXT, '-', LPAD(month_occurred::TEXT, 2, '0'), '-01'),
        'YYYY-MM-DD'
    )                                                     AS month_key,
    company_name,
    division_name,
    department_name,
    COUNT(DISTINCT case_external_id)                      AS personal_lti_count
FROM {{ ref('cases_details') }}
WHERE kpi_id = '9025'
GROUP BY year_occurred, month_occurred, company_name, division_name, department_name
ORDER BY year_occurred, month_occurred, company_name, division_name, department_name
