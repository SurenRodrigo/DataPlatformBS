WITH employee_metrics AS (
    SELECT
        year,
        month,
        month_name,
        company                                                     AS company_name,
        division                                                    AS division_name,
        SUM(
            CASE WHEN metric = 'Opening Balance' THEN count ELSE 0 END
        )                                                           AS opening_balance,
        SUM(
            CASE WHEN metric = 'Closing Balance' THEN count ELSE 0 END
        )                                                           AS closing_balance,
        SUM(CASE WHEN metric = 'Joiners' THEN count ELSE 0 END)     AS joiners,
        SUM(CASE WHEN metric = 'Leavers' THEN count ELSE 0 END)     AS leavers,
        SUM(
            CASE WHEN metric = 'Voluntary Leavers' THEN count ELSE 0 END
        )                                                           AS voluntary_leavers,
        SUM(
            CASE WHEN metric = 'Internal Moves' THEN count ELSE 0 END
        )                                                           AS internal_moves,
        SUM(
            CASE WHEN metric = 'Women Employees' THEN count ELSE 0 END
        )                                                           AS women_employees,
        SUM(
            CASE WHEN metric = 'Temp Employees' THEN count ELSE 0 END
        )                                                           AS temp_employees,
        SUM(CASE WHEN metric = 'Apprentices' THEN count ELSE 0 END) AS apprentices
    FROM {{ ref('hr_employee_metrics_detail') }}
    WHERE
        metric IN (
            'Opening Balance',
            'Closing Balance',
            'Joiners',
            'Leavers',
            'Voluntary Leavers',
            'Internal Moves', 'Women Employees', 'Temp Employees', 'Apprentices'
        )
    GROUP BY year, month, month_name, company, division
),

manual_inputs AS (
    SELECT
        year,
        month,
        month_name,
        company_name,
        division_name,
        SUM(
            CASE WHEN metric = 'Rented Personal Count' THEN COALESCE(value, 0) ELSE 0 END
        ) AS rented_personal_count,
        SUM(
            CASE
                WHEN metric = 'Rented Workers Total Hours' THEN COALESCE(value, 0) ELSE 0
            END
        ) AS rented_worker_hours,
        SUM(
            CASE
                WHEN metric = 'Subcontractor Total Hours' THEN COALESCE(value, 0) ELSE 0
            END
        ) AS subcontractor_hours
    FROM {{ ref('hr_manual_inputs') }}
    WHERE
        metric IN (
            'Rented Personal Count',
            'Rented Workers Total Hours',
            'Subcontractor Total Hours'
        )
    GROUP BY year, month, month_name, company_name, division_name
),

open_roles AS (
    SELECT
        year,
        month,
        month_name,
        company_name,
        division_name,
        SUM(COALESCE(open_vacancy_count, 0)) AS total_open_roles
    FROM {{ ref('int__open_roles_recruitment') }}
    GROUP BY year, month, month_name, company_name, division_name
),

lti_personal AS (
    SELECT
        year,
        month,
        month_name,
        company_name,
        division_name,
        SUM(COALESCE(personal_lti_count, 0)) AS total_lti_personal_days
    FROM {{ ref('int__nrc_personal_lti_days') }}
    GROUP BY year, month, month_name, company_name, division_name
),

metrics_base AS (
    SELECT
        emp_metrics.year,
        emp_metrics.month,
        emp_metrics.month_name,
        TO_CHAR(
            TO_DATE(emp_metrics.year || '-' || emp_metrics.month || '-01', 'YYYY-MM-DD'),
            'Mon YYYY'
        )               AS month_label,
        TO_DATE(
            CONCAT(emp_metrics.year, '-', emp_metrics.month, '-01'), 'YYYY-MM-DD'
        )               AS month_key,
        emp_metrics.company_name,
        emp_metrics.division_name,
        COALESCE(
            emp_metrics.opening_balance, 0
        )               AS opening_balance,
        COALESCE(
            emp_metrics.closing_balance, 0
        )               AS closing_balance,
        COALESCE(
            emp_metrics.joiners, 0
        )               AS joiners,
        COALESCE(
            emp_metrics.leavers, 0
        )               AS leavers,
        COALESCE(
            emp_metrics.voluntary_leavers, 0
        )               AS voluntary_leavers,
        COALESCE(
            emp_metrics.internal_moves, 0
        )               AS internal_moves,
        COALESCE(
            emp_metrics.women_employees, 0
        )               AS women_employees,
        COALESCE(
            emp_metrics.temp_employees, 0
        )               AS temp_employees,
        COALESCE(
            emp_metrics.apprentices, 0
        )               AS apprentices,
        COALESCE(
            manual_inputs.rented_personal_count, 0
        )               AS rented_personal_count,
        COALESCE(
            manual_inputs.rented_worker_hours, 0
        )               AS rented_worker_hours,
        COALESCE(
            manual_inputs.subcontractor_hours, 0
        )               AS subcontractor_hours,
        COALESCE(
            open_roles.total_open_roles, 0
        )               AS total_open_roles,
        COALESCE(
            lti_personal.total_lti_personal_days, 0
        )               AS total_lti_personal_days,
        NULL::varchar   AS time_category,
        NULL::numeric   AS hours,
        NULL::timestamp AS modified_date_time,
        NULL::varchar   AS data_source_name
    FROM employee_metrics AS emp_metrics
    LEFT JOIN manual_inputs AS manual_inputs
        ON
            emp_metrics.year = manual_inputs.year
            AND emp_metrics.month = manual_inputs.month
            AND emp_metrics.company_name = manual_inputs.company_name
            AND emp_metrics.division_name = manual_inputs.division_name
    LEFT JOIN open_roles AS open_roles
        ON
            emp_metrics.year = open_roles.year
            AND emp_metrics.month = open_roles.month
            AND emp_metrics.company_name = open_roles.company_name
            AND emp_metrics.division_name = open_roles.division_name
    LEFT JOIN lti_personal AS lti_personal
        ON
            emp_metrics.year = lti_personal.year
            AND emp_metrics.month = lti_personal.month
            AND emp_metrics.company_name = lti_personal.company_name
            AND emp_metrics.division_name = lti_personal.division_name
),

time_records_base AS (
    SELECT
        work_year       AS year,
        work_month      AS month,
        work_month_name AS month_name,
        TO_CHAR(
            TO_DATE(work_year || '-' || work_month || '-01', 'YYYY-MM-DD'), 'Mon YYYY'
        )               AS month_label,
        TO_DATE(
            CONCAT(work_year, '-', work_month, '-01'), 'YYYY-MM-DD'
        )               AS month_key,
        company_name,
        division_name,
        NULL::numeric   AS opening_balance,
        NULL::numeric   AS closing_balance,
        NULL::numeric   AS joiners,
        NULL::numeric   AS leavers,
        NULL::numeric   AS voluntary_leavers,
        NULL::numeric   AS internal_moves,
        NULL::numeric   AS women_employees,
        NULL::numeric   AS temp_employees,
        NULL::numeric   AS apprentices,
        NULL::numeric   AS rented_personal_count,
        NULL::numeric   AS rented_worker_hours,
        NULL::numeric   AS subcontractor_hours,
        NULL::numeric   AS total_open_roles,
        NULL::numeric   AS total_lti_personal_days,
        time_category,
        hours,
        modified_date_time,
        data_source_name
    FROM {{ ref('hr_time_records') }}
)

SELECT * FROM metrics_base
UNION ALL
SELECT * FROM time_records_base
