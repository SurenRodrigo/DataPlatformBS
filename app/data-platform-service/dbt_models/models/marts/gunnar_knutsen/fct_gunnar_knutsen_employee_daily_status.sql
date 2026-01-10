WITH dimension_calendar AS (
    SELECT
        date_key,
        year,
        month,
        month_name
    FROM {{ ref('int_gk_gen_date_spine') }}
),

employee AS (
    SELECT
        ext_employee_id::TEXT AS employee_id,
        company_id,
        company_name,
        legal_entity_id,
        legal_entity_name,
        organizational_unit_id,
        organizational_unit_name,
        employment_date,
        employment_end_date,
        gender
    FROM {{ ref('dim_gunnar_knutsen_admmit_employee') }}
),

employee_dates AS (
    SELECT
        dimension_calendar.date_key,
        dimension_calendar.year,
        dimension_calendar.month,
        dimension_calendar.month_name,
        employee.employee_id,
        employee.gender,
        NULL AS reason_for_leaving,
        employee.company_id,
        employee.company_name,
        employee.legal_entity_id,
        employee.legal_entity_name,
        employee.organizational_unit_id,
        employee.organizational_unit_name,
        employee.employment_date,
        employee.employment_end_date
    FROM dimension_calendar
    CROSS JOIN employee
    WHERE
        dimension_calendar.date_key >= employee.employment_date
        AND (
            employee.employment_end_date IS NULL
            OR dimension_calendar.date_key <= employee.employment_end_date
        )
)

SELECT
    employee_dates.date_key,
    employee_dates.year,
    employee_dates.month,
    employee_dates.month_name,
    employee_dates.employee_id,
    employee_dates.gender,
    -- GK employees are always active during their employment period
    TRUE
        AS is_active,
    -- Not available for GK
    NULL
        AS active_group,
    -- Not available for GK
    NULL
        AS active_relationship,
    employee_dates.company_name
        AS active_company,
    employee_dates.company_id
        AS active_company_id,
    employee_dates.legal_entity_name
        AS active_division,
    employee_dates.legal_entity_id
        AS active_division_id,
    employee_dates.organizational_unit_name
        AS active_department,
    employee_dates.organizational_unit_id
        AS active_department_id,
    COALESCE(employee_dates.gender = 'Male', FALSE)                                  AS is_male,

    COALESCE(employee_dates.gender = 'Female', FALSE)                                AS is_female,

    -- Not available for GK
    FALSE
        AS is_apprentice,

    -- Not available for GK
    FALSE
        AS is_temp_employee,

    -- Not available for GK
    FALSE
        AS is_white_collar,

    -- Not available for GK
    FALSE
        AS is_blue_collar,

    COALESCE(employee_dates.employment_date = employee_dates.date_key, FALSE)     AS is_joiner,

    COALESCE(employee_dates.employment_end_date = employee_dates.date_key, FALSE) AS is_leaver,

    -- Not available for GK
    FALSE
        AS is_voluntary_leaver,

    -- Not available for GK
    FALSE
        AS is_retired

FROM employee_dates

ORDER BY date_key ASC, employee_id ASC
