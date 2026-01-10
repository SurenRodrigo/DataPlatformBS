WITH employee_data AS (
    SELECT DISTINCT
        active_company       AS company_name,
        active_company_id    AS company_id,
        active_division_id   AS division_id,
        active_department_id AS department_id,
        active_division      AS division_name,
        active_department    AS department_name,
        employee_id,
        year,
        month
    FROM {{ ref('fct_employee_daily_status') }}
    WHERE is_active
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
        modified_date_time,
        is_ordinary_hours,
        is_overtime,
        is_sick_leave
    FROM {{ ref('time_record') }}
),

project_external_inputs AS (
    SELECT
        company_id,
        company_name,
        division_id,
        division_name,
        organizational_unit_id   AS department_id,
        organizational_unit_name AS department_name,
        project_name,
        project_number,
        year,
        month,
        external_input_type_id,
        value
    FROM {{ ref('project_external_inputs') }}
),

hours_count AS (
    SELECT
        company_id,
        company_name,
        division_id,
        division_name,
        department_id,
        department_name,
        project_number,
        project_name,
        modified_date_time,
        year,
        month,
        UPPER(TO_CHAR(date::DATE, 'MON'))                            AS month_name,
        SUM(CASE WHEN is_ordinary_hours THEN total_hours ELSE 0 END) AS total_hours,
        SUM(
            CASE WHEN is_overtime THEN total_hours ELSE 0 END
        )                                                            AS total_overtime_hours,
        SUM(
            CASE WHEN is_sick_leave THEN total_hours ELSE 0 END
        )                                                            AS total_sick_leave_hours
    FROM time_record_data
    GROUP BY
        company_id,
        company_name,
        division_id,
        division_name,
        department_id,
        department_name,
        project_number,
        project_name,
        year,
        month,
        date,
        modified_date_time
)

SELECT * FROM hours_count
ORDER BY division_name, department_name, project_number, year, month
