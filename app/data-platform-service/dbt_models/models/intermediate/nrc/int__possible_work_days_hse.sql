WITH employee_data AS (
    SELECT
        active_company       AS company_name,
        active_company_id    AS company_id,
        active_division      AS division_name,
        active_division_id   AS division_id,
        active_department    AS department_name,
        active_department_id AS department_id,
        year,
        month,
        month_name,
        employee_id,
        is_active,
        is_joiner,
        is_leaver,
        is_voluntary_leaver,
        is_retired,
        is_female,
        is_male,
        is_white_collar,
        is_blue_collar,
        is_apprentice,
        is_temp_employee,
        date_key
    FROM {{ ref('fct_employee_daily_status') }}
    WHERE active_division IS NOT NULL AND active_company IS NOT NULL
),

time_record_data AS (
    SELECT
        company_id,
        company_name,
        division_id,
        division_name,
        organizational_unit_id   AS department_id,
        organizational_unit_name AS department_name,
        project_id               AS project_number,
        project_name,
        hours                    AS total_hours,
        date,
        ext_employee_id          AS employee_id,
        time_type,
        work_year::INT           AS year,
        work_month               AS month,
        is_ordinary_hours,
        is_overtime,
        is_sick_leave
    FROM {{ ref('time_record') }}
),

department_workdays AS (
    SELECT
        company_id,
        company_name,
        division_id,
        division_name,
        department_id,
        department_name,
        NULL::INT                                  AS project_number,
        NULL                                       AS project_name,
        year,
        month,
        TO_CHAR(TO_DATE(month::TEXT, 'MM'), 'MON') AS month_name,
        COUNT(employee_id) * 22                    AS department_possible_workdays
    FROM (
        SELECT
            mon.company_id,
            mon.company_name,
            mon.division_id,
            mon.division_name,
            mon.department_id,
            mon.department_name,
            mon.year,
            mon.month,
            mon.month_name,
            mon.employee_id,
            ROW_NUMBER() OVER (
                PARTITION BY
                    mon.company_id, mon.company_name,
                    mon.division_id, mon.division_name,
                    mon.department_id, mon.department_name,
                    mon.year, mon.month, mon.employee_id
                ORDER BY mon.date_key DESC
            ) AS row_num
        FROM employee_data AS mon
        WHERE mon.is_active = TRUE
    ) AS dedup
    WHERE row_num = 1
    GROUP BY
        company_id,
        company_name,
        division_id,
        division_name,
        department_id,
        department_name,
        year,
        month,
        month_name
),

division_workdays AS (
    SELECT
        company_id,
        company_name,
        division_id,
        division_name,
        NULL::INT                                  AS department_id,
        NULL                                       AS department_name,
        NULL::INT                                  AS project_number,
        NULL                                       AS project_name,
        year,
        month,
        TO_CHAR(TO_DATE(month::TEXT, 'MM'), 'MON') AS month_name,
        COUNT(employee_id) * 22                    AS division_possible_workdays
    FROM (
        SELECT
            mon.company_id,
            mon.company_name,
            mon.division_id,
            mon.division_name,
            mon.year,
            mon.month,
            mon.month_name,
            mon.employee_id,
            ROW_NUMBER() OVER (
                PARTITION BY
                    mon.company_id, mon.company_name,
                    mon.division_id, mon.division_name,
                    mon.year, mon.month, mon.employee_id
                ORDER BY mon.date_key DESC
            ) AS row_num
        FROM employee_data AS mon
        WHERE mon.is_active = TRUE
    ) AS dedup
    WHERE row_num = 1
    GROUP BY
        company_id,
        company_name,
        division_id,
        division_name,
        year,
        month,
        month_name
),

project_workdays AS (
    SELECT
        company_id,
        company_name,
        division_id,
        division_name,
        department_name,
        department_id,
        project_number,
        project_name,
        year,
        month,
        TO_CHAR(TO_DATE(month::TEXT, 'MM'), 'MON') AS month_name,
        COUNT(DISTINCT employee_id) * 22           AS project_possible_workdays
    FROM time_record_data
    WHERE project_number IS NOT NULL
    GROUP BY
        company_id,
        company_name,
        division_name,
        division_id,
        department_name,
        department_id,
        project_number,
        project_name,
        year,
        month
),

possible_work_days AS (
    SELECT
        COALESCE(
            project_workdays.company_id,
            department_workdays.company_id,
            division_workdays.company_id
        ) AS company_id,
        COALESCE(
            project_workdays.company_name,
            department_workdays.company_name,
            division_workdays.company_name
        ) AS company_name,
        COALESCE(
            project_workdays.division_id,
            department_workdays.division_id,
            division_workdays.division_id
        ) AS division_id,
        COALESCE(
            project_workdays.division_name,
            department_workdays.division_name,
            division_workdays.division_name
        ) AS division_name,
        COALESCE(
            project_workdays.department_name,
            department_workdays.department_name,
            division_workdays.department_name
        ) AS department_name,
        COALESCE(
            project_workdays.department_id,
            department_workdays.department_id,
            division_workdays.department_id
        ) AS department_id,
        COALESCE(
            project_workdays.project_number,
            department_workdays.project_number,
            division_workdays.project_number
        ) AS project_number,
        COALESCE(
            project_workdays.project_name,
            department_workdays.project_name,
            division_workdays.project_name
        ) AS project_name,
        COALESCE(
            project_workdays.year, department_workdays.year, division_workdays.year
        ) AS year,
        COALESCE(
            project_workdays.month, department_workdays.month, division_workdays.month
        ) AS month,
        COALESCE(
            project_workdays.month_name,
            department_workdays.month_name,
            division_workdays.month_name
        ) AS month_name,
        COALESCE(
            department_workdays.department_possible_workdays, 0
        ) AS department_possible_workdays,
        COALESCE(
            project_workdays.project_possible_workdays, 0
        ) AS project_possible_workdays,
        COALESCE(
            division_workdays.division_possible_workdays, 0
        ) AS division_possible_workdays
    FROM project_workdays
    FULL JOIN department_workdays
        ON
            project_workdays.division_name = department_workdays.division_name
            AND project_workdays.department_name = department_workdays.department_name
            AND project_workdays.project_name = department_workdays.project_name
            AND project_workdays.year = department_workdays.year
            AND project_workdays.month = department_workdays.month
    FULL JOIN division_workdays
        ON
            project_workdays.division_name = division_workdays.division_name
            AND project_workdays.department_name = division_workdays.department_name
            AND project_workdays.project_name = division_workdays.project_name
            AND project_workdays.year = division_workdays.year
            AND project_workdays.month = division_workdays.month
)

SELECT * FROM possible_work_days
