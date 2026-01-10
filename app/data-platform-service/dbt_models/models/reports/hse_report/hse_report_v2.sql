WITH unique_cases AS (
    SELECT
        company_id,
        company_name,
        division_id,
        division_name,
        department_id,
        department_name,
        project_number,
        project_level_name,
        project_id,
        date_occurred,
        year_occurred,
        month_occurred,
        month,
        case_external_id,
        case_type_id,
        kpi_id
    FROM {{ ref('cases_details') }}
),

kpi_counts AS (
    SELECT
        company_id,
        company_name,
        division_id,
        division_name,
        department_id,
        department_name,
        project_number,
        project_level_name,
        date_occurred,
        year_occurred,
        month_occurred,
        month,
        SUM(
            CASE
                WHEN (kpi_id = '9025') THEN 1
                ELSE 0.0
            END
        ) AS h1_count,
        SUM(
            CASE
                WHEN (kpi_id = '9024') THEN 1
                ELSE 0.0
            END
        ) AS h2_count,
        SUM(
            CASE
                WHEN (kpi_id = '9024' OR kpi_id = '9025') THEN 1
                ELSE 0.0
            END
        ) AS count_of_h1_and_h2_cases,
        SUM(
            CASE
                WHEN kpi_id = '9178' THEN 1
                ELSE 0.0
            END
        ) AS m1_count,
        SUM(
            CASE
                WHEN kpi_id = '9177' THEN 1
                ELSE 0.0
            END
        ) AS m2_count,
        SUM(
            CASE
                WHEN kpi_id = '9176' THEN 1
                ELSE 0.0
            END
        ) AS m3_count,
        SUM(
            CASE
                WHEN (kpi_id = '9178' OR kpi_id = '9177' OR kpi_id = '9176') THEN 1
                ELSE 0.0
            END
        ) AS count_of_m1_m2_and_m3_cases
    FROM unique_cases
    GROUP BY
        company_id,
        company_name,
        division_id,
        division_name,
        department_id,
        department_name,
        project_number,
        project_level_name,
        year_occurred,
        month_occurred,
        month,
        date_occurred
),

manual_inputs AS (
    SELECT
        company_id,
        company_name,
        division_id,
        division_name,
        organizational_unit_id                     AS department_id,
        organizational_unit_name                   AS department_name,
        NULL                                       AS project_number,
        NULL                                       AS project_name,
        year,
        month,
        TO_CHAR(TO_DATE(month::TEXT, 'MM'), 'MON') AS month_name,
        SUM(
            CASE
                WHEN external_input_type_id = '1' THEN value
                ELSE 0.0
            END
        )                                          AS rented_workers_hours,
        SUM(
            CASE
                WHEN external_input_type_id = '2' THEN value
                ELSE 0.0
            END
        )                                          AS subcontractor_worked_hours,
        SUM(
            CASE
                WHEN external_input_type_id = '5' THEN value
                ELSE 0.0
            END
        )                                          AS residual_waste,
        SUM(
            CASE
                WHEN external_input_type_id = '6' THEN value
                ELSE 0.0
            END
        )                                          AS total_waste,
        SUM(
            CASE
                WHEN external_input_type_id = '4' THEN value
                ELSE 0.0
            END
        )                                          AS no_of_absence_days_due_to_personal_injury
    FROM
        {{ ref('project_external_inputs') }}
    GROUP BY
        company_id,
        company_name,
        division_id,
        division_name,
        organizational_unit_id,
        organizational_unit_name,
        year,
        month
),

possible_work_days AS (
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
        month_name,
        department_possible_workdays AS possible_workdays,
        project_possible_workdays AS actual_project_workdays
    FROM {{ ref('int__possible_work_days_hse') }}
),

monthly_records AS (
    SELECT
        COALESCE(
            kpi_counts.company_id, manual_inputs.company_id, possible_work_days.company_id
        ) AS company_id,
        COALESCE(
            kpi_counts.company_name,
            manual_inputs.company_name,
            possible_work_days.company_name
        ) AS company_name,
        COALESCE(
            kpi_counts.division_id,
            manual_inputs.division_id,
            possible_work_days.division_id
        ) AS division_id,
        COALESCE(
            kpi_counts.division_name,
            manual_inputs.division_name,
            possible_work_days.division_name
        ) AS division_name,
        COALESCE(
            kpi_counts.department_id,
            manual_inputs.department_id,
            possible_work_days.department_id
        ) AS department_id,
        COALESCE(
            kpi_counts.department_name,
            manual_inputs.department_name,
            possible_work_days.department_name
        ) AS department_name,
        COALESCE(
            kpi_counts.project_number::TEXT,
            manual_inputs.project_number::TEXT,
            possible_work_days.project_number::TEXT
        ) AS project_number,
        COALESCE(
            kpi_counts.project_level_name,
            manual_inputs.project_name,
            possible_work_days.project_name
        ) AS project_name,
        COALESCE(
            kpi_counts.year_occurred, manual_inputs.year, possible_work_days.year
        ) AS year_occurred,
        COALESCE(
            kpi_counts.month_occurred, manual_inputs.month, possible_work_days.month
        ) AS month_occurred,
        COALESCE(
            kpi_counts.month, manual_inputs.month_name, possible_work_days.month_name
        ) AS month,
        COALESCE(
            kpi_counts.h1_count, 0
        ) AS h1_count,
        COALESCE(
            kpi_counts.h2_count, 0
        ) AS h2_count,
        COALESCE(
            kpi_counts.count_of_h1_and_h2_cases, 0
        ) AS total_h1_h2_count,
        COALESCE(
            kpi_counts.m1_count, 0
        ) AS m1_count,
        COALESCE(
            kpi_counts.m2_count, 0
        ) AS m2_count,
        COALESCE(
            kpi_counts.m3_count, 0
        ) AS m3_count,
        COALESCE(kpi_counts.date_occurred, null) AS date_occurred,
        COALESCE(
            kpi_counts.count_of_m1_m2_and_m3_cases, 0
        ) AS total_m1_m2_m3_count,
        COALESCE(
            manual_inputs.residual_waste, 0
        ) AS residual_waste,
        COALESCE(
            manual_inputs.total_waste, 0
        ) AS total_waste,
        COALESCE(
            manual_inputs.subcontractor_worked_hours, 0
        ) AS subcontractor_worked_hours,
        COALESCE(
            manual_inputs.rented_workers_hours, 0
        ) AS rented_workers_hours,
        COALESCE(
            manual_inputs.no_of_absence_days_due_to_personal_injury, 0
        ) AS no_of_absence_days_due_to_personal_injury,
        COALESCE(
            possible_work_days.possible_workdays, 0
        ) AS department_workdays,
        COALESCE(
            possible_work_days.actual_project_workdays, 0
        ) AS project_workdays
    FROM kpi_counts
    FULL JOIN manual_inputs
        ON
            kpi_counts.year_occurred = manual_inputs.year
            AND kpi_counts.month_occurred = manual_inputs.month
            AND kpi_counts.division_id = manual_inputs.division_id
            AND kpi_counts.department_id = manual_inputs.department_id
    -- AND kpi_counts.project_number = manual_inputs.project_number  
    FULL JOIN possible_work_days
        ON
            kpi_counts.year_occurred = possible_work_days.year
            AND kpi_counts.month_occurred = possible_work_days.month
            AND kpi_counts.division_id = possible_work_days.division_id
            AND kpi_counts.department_id = possible_work_days.department_id
-- AND kpi_counts.project_number = possible_work_days.project_number     
)

SELECT * FROM monthly_records
