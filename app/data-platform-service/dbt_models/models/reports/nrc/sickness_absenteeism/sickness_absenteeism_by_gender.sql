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
genders AS (
    SELECT * FROM (VALUES ('Mann'), ('Kvinne')) AS t (gender)
),
leave_types AS (
    SELECT * FROM (
        VALUES 
            ('Sick Leave with Doctor''s Note'),
            ('Self Administered Sick Leave')
    ) AS t (leave_type)
),
all_combinations AS (
    SELECT
        date_range.generated_date,
        org_structure.company_name,
        org_structure.division_name,
        org_structure.organizational_unit_name,
        org_structure.project_name,
        genders.gender,
        leave_types.leave_type
    FROM date_range
    CROSS JOIN org_structure
    CROSS JOIN genders
    CROSS JOIN leave_types
),
sick_leaves AS (
    SELECT
        time_record.date::DATE AS sick_leave_date,
        time_record.company_name,
        time_record.division_name,
        time_record.organizational_unit_name,
        time_record.project_name,
        employee.gender,
        CASE
            WHEN time_record.time_type = 'SYKE' THEN 'Self Administered Sick Leave'
            WHEN time_record.time_type IN ('SYKA', 'SYKL', 'SYKM') THEN 'Sick Leave with Doctor''s Note'
        END AS leave_type,
        COUNT(DISTINCT time_record.ext_employee_id) AS employee_count
    FROM {{ ref('time_record') }} AS time_record
    LEFT JOIN {{ ref('employee') }} AS employee
        ON time_record.ext_employee_id = employee.employee_id
    WHERE
        time_record.time_type IN ('SYKA', 'SYKE', 'SYKL', 'SYKM')
        AND (employee.gender IS NOT NULL AND employee.gender != '')
    GROUP BY
        time_record.date::DATE,
        time_record.company_name,
        time_record.division_name,
        time_record.organizational_unit_name,
        time_record.project_name,
        employee.gender,
        leave_type
)
SELECT
    all_combinations.generated_date AS absence_date,
    all_combinations.company_name,
    all_combinations.division_name,
    all_combinations.organizational_unit_name AS department_name,
    all_combinations.project_name,
    CASE
        WHEN all_combinations.gender = 'Mann' THEN 'Male'
        WHEN all_combinations.gender = 'Kvinne' THEN 'Female'
        ELSE all_combinations.gender
    END AS gender,
    all_combinations.leave_type,
    CASE
        WHEN all_combinations.gender = 'Mann' THEN 'Male - ' || all_combinations.leave_type
        WHEN all_combinations.gender = 'Kvinne' THEN 'Female - ' || all_combinations.leave_type
        ELSE all_combinations.gender || ' - ' || all_combinations.leave_type
    END AS type,
    COALESCE(sick_leaves.employee_count, 0) AS employee_count
FROM all_combinations
LEFT JOIN sick_leaves
    ON all_combinations.generated_date = sick_leaves.sick_leave_date
    AND all_combinations.company_name = sick_leaves.company_name
    AND all_combinations.division_name = sick_leaves.division_name
    AND all_combinations.organizational_unit_name = sick_leaves.organizational_unit_name
    AND all_combinations.project_name = sick_leaves.project_name
    AND all_combinations.gender = sick_leaves.gender
    AND all_combinations.leave_type = sick_leaves.leave_type