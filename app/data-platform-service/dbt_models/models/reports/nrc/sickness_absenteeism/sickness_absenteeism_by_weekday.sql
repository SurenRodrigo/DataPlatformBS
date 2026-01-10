WITH date_range AS (
    SELECT GENERATE_SERIES(
        (SELECT MIN(date::DATE) FROM {{ ref('time_record') }}),
        (SELECT MAX(date::DATE) FROM {{ ref('time_record') }}),
        INTERVAL '1 day'
    )::DATE AS generated_date
),

org_structure AS (
    SELECT DISTINCT
        company_name,
        division_name,
        organizational_unit_name,
        project_name
    FROM {{ ref('time_record') }}
),

all_combinations AS (
    SELECT
        date_range.generated_date,
        org_structure.company_name,
        org_structure.division_name,
        org_structure.organizational_unit_name,
        org_structure.project_name
    FROM date_range
    CROSS JOIN org_structure
),

sick_leaves AS (
    SELECT
        time_record.date::DATE                      AS sick_leave_date,
        time_record.company_name,
        time_record.division_name,
        time_record.organizational_unit_name,
        time_record.project_name,
        CASE
            WHEN time_record.time_type = 'SYKE' THEN 'Self Administered Sick Leave'
            WHEN time_record.time_type IN ('SYKA', 'SYKM', 'SYKL') THEN 'Sick Leave with Doctor''s Note'
        END AS leave_type,
        COUNT(DISTINCT time_record.ext_employee_id) AS employee_count
    FROM {{ ref('time_record') }} AS time_record
    LEFT JOIN {{ ref('employee') }} AS employee
        ON time_record.ext_employee_id = employee.employee_id
    WHERE
        time_record.time_type IN ('SYKA', 'SYKE', 'SYKL', 'SYKM')
    GROUP BY
        time_record.date::DATE,
        time_record.company_name,
        time_record.division_name,
        time_record.organizational_unit_name,
        time_record.project_name,
        leave_type
)

SELECT
    all_combinations.generated_date                AS absence_date,
    all_combinations.company_name,
    all_combinations.division_name,
    all_combinations.organizational_unit_name      AS department_name,
    all_combinations.project_name,
    TO_CHAR(all_combinations.generated_date, 'Day') AS weekday_name,
    EXTRACT(DOW FROM all_combinations.generated_date) AS weekday_number,
    sick_leaves.leave_type,
    COALESCE(sick_leaves.employee_count, 0)        AS employee_count
FROM all_combinations
LEFT JOIN sick_leaves
    ON all_combinations.generated_date = sick_leaves.sick_leave_date
    AND all_combinations.company_name = sick_leaves.company_name
    AND all_combinations.division_name = sick_leaves.division_name
    AND all_combinations.organizational_unit_name = sick_leaves.organizational_unit_name
    AND all_combinations.project_name = sick_leaves.project_name
