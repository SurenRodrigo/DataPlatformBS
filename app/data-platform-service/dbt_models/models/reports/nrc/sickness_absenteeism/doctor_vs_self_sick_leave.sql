WITH month_range AS (
    SELECT DATE_TRUNC('month', date_day)::DATE AS generated_month
    FROM {{ ref('int__gen_date_spine') }}
    GROUP BY DATE_TRUNC('month', date_day)
),

company_divisions AS (
    SELECT
        time_record.company_name,
        time_record.division_name
    FROM {{ ref('time_record') }} AS time_record
    WHERE
        time_record.company_name IS NOT NULL
        AND time_record.division_name IS NOT NULL
    GROUP BY
        time_record.company_name,
        time_record.division_name
),

sick_leave_metrics AS (
    SELECT UNNEST(ARRAY[
        'Short Term Sick Leave with a Doctors Note',
        'Long Term Sick Leave with a Doctors Note',
        'Self Administered Sick Leave'
    ]) AS metric
),

sick_leaves AS (
    SELECT
        DATE_TRUNC('month', time_record.date::DATE)::DATE AS generated_month,
        time_record.company_name,
        time_record.division_name,
        time_record.organizational_unit_name              AS department_name,
        time_record.project_name,
        time_record.ext_employee_id                       AS employee_id,
        CASE
            WHEN
                time_record.time_type IN ('SYKM', 'SYKA')
                THEN 'Short Term Sick Leave with a Doctors Note'
            WHEN
                time_record.time_type = 'SYKL'
                THEN 'Long Term Sick Leave with a Doctors Note'
            WHEN time_record.time_type = 'SYKE' THEN 'Self Administered Sick Leave'
        END                                               AS metric
    FROM {{ ref('time_record') }} AS time_record
    WHERE time_record.time_type IN ('SYKA', 'SYKE', 'SYKL', 'SYKM')
    GROUP BY
        DATE_TRUNC('month', time_record.date::DATE),
        time_record.company_name,
        time_record.division_name,
        time_record.organizational_unit_name,
        time_record.project_name,
        time_record.ext_employee_id,
        CASE
            WHEN
                time_record.time_type IN ('SYKM', 'SYKA')
                THEN 'Short Term Sick Leave with a Doctors Note'
            WHEN
                time_record.time_type = 'SYKL'
                THEN 'Long Term Sick Leave with a Doctors Note'
            WHEN time_record.time_type = 'SYKE' THEN 'Self Administered Sick Leave'
        END
),

missing_combinations AS (
    SELECT
        month_range.generated_month,
        company_divisions.company_name,
        company_divisions.division_name,
        NULL AS department_name,
        NULL AS project_name,
        NULL AS employee_id,
        sick_leave_metrics.metric
    FROM sick_leave_metrics
    CROSS JOIN month_range
    CROSS JOIN company_divisions
    LEFT JOIN sick_leaves AS sick_leave
        ON
            sick_leave_metrics.metric = sick_leave.metric
            AND company_divisions.company_name = sick_leave.company_name
            AND company_divisions.division_name = sick_leave.division_name
            AND month_range.generated_month = sick_leave.generated_month
    WHERE sick_leave.employee_id IS NULL
),

final_rows AS (
    SELECT
        generated_month,
        company_name,
        division_name,
        department_name,
        project_name,
        employee_id,
        metric
    FROM sick_leaves

    UNION ALL

    SELECT
        generated_month,
        company_name,
        division_name,
        department_name,
        project_name,
        employee_id,
        metric
    FROM missing_combinations
)

SELECT
    TO_CHAR(generated_month, 'YYYY')        AS year,
    EXTRACT(MONTH FROM generated_month)     AS month,
    TRIM(TO_CHAR(generated_month, 'Month')) AS month_name,
    TO_CHAR(generated_month, 'Mon YYYY')    AS month_label,
    generated_month                         AS month_key,
    company_name,
    division_name,
    department_name,
    project_name,
    employee_id,
    metric
FROM final_rows

ORDER BY
    employee_id
