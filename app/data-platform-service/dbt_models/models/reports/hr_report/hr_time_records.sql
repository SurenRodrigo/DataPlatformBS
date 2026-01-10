WITH actual_records AS (
    SELECT
        record_id,
        ext_employee_id AS employee_number,
        hours,
        project_id,
        company_name,
        division_name,
        organizational_unit_name,
        date            AS work_date,
        TO_CHAR(
            TO_DATE(work_year || '-' || work_month || '-01', 'YYYY-MM-DD'),
            'Mon YYYY'
        )               AS month_label,
        TO_DATE(
            CONCAT(work_year, '-', work_month, '-01'), 'YYYY-MM-DD'
        )               AS month_key,
        modified_date_time,
        time_type,
        data_source_name,
        time_status_code,
        work_year,
        work_month_name,
        work_month,
        CASE
            WHEN is_ordinary_hours = TRUE THEN 'Ordinary'
            WHEN is_sick_leave = TRUE THEN 'Sick Leave'
            WHEN is_overtime = TRUE THEN 'Overtime'
            ELSE 'Other'
        END             AS time_category,
        CASE
            WHEN is_ordinary_hours = TRUE THEN 'Total Worked Hours (Own)'
            WHEN is_sick_leave = TRUE THEN 'Total Sick Leave Hours'
            WHEN is_overtime = TRUE THEN 'Total Overtime Hours'
            ELSE 'Other Hours'
        END             AS metric
    FROM {{ ref('time_record') }}
    WHERE
        work_year IS NOT NULL
        AND work_month IS NOT NULL
        AND company_name IS NOT NULL
        AND division_name IS NOT NULL
),


all_year_months AS (
    SELECT
        year::TEXT AS year,
        month,
        month_name
    FROM {{ ref('int__gen_date_spine') }}
    GROUP BY year, month, month_name
),

company_divisions AS (
    SELECT
        company_name,
        division_name
    FROM actual_records
    GROUP BY company_name, division_name
),

dimensions AS (
    SELECT
        all_year_months.year       AS work_year,
        all_year_months.month      AS work_month,
        all_year_months.month_name AS work_month_name,
        company_divisions.company_name,
        company_divisions.division_name
    FROM all_year_months
    CROSS JOIN company_divisions
),

time_categories AS (
    SELECT 'Ordinary' AS time_category
    UNION ALL
    SELECT 'Sick Leave'
    UNION ALL
    SELECT 'Overtime'
    UNION ALL
    SELECT 'Other'
),

data_sources AS (
    SELECT 'iCore' AS data_source_name
    UNION ALL
    SELECT 'Ditio'
),

all_combinations AS (
    SELECT
        dimensions.work_year,
        dimensions.work_month,
        dimensions.work_month_name,
        dimensions.company_name,
        dimensions.division_name,
        data_sources.data_source_name,
        time_categories.time_category
    FROM dimensions
    CROSS JOIN time_categories
    CROSS JOIN data_sources
),

--Creating zero-hour records for missing combinations of division-month-time_category-data_source
zero_hour_records AS (
    SELECT
        'No records'                     AS record_id,
        NULL                             AS employee_number,
        0                                AS hours,
        NULL::INT                        AS project_id,
        all_combinations.company_name,
        all_combinations.division_name,
        NULL                             AS organizational_unit_name,
        (
            all_combinations.work_year
            || '-'
            || LPAD(all_combinations.work_month::TEXT, 2, '0')
            || '-01 00:00:00'
        )::TIMESTAMP                     AS work_date,
        TO_CHAR(
            TO_DATE(
                all_combinations.work_year || '-' || all_combinations.work_month || '-01',
                'YYYY-MM-DD'
            ),
            'Mon YYYY'
        )                                AS month_label,
        TO_DATE(
            CONCAT(all_combinations.work_year, '-', all_combinations.work_month, '-01'),
            'YYYY-MM-DD'
        )                                AS month_key,
        '0001-01-01 00:00:00'::TIMESTAMP AS modified_date_time,
        NULL                             AS time_type,
        all_combinations.data_source_name,
        NULL                             AS time_status_code,
        all_combinations.work_year,
        all_combinations.work_month_name,
        all_combinations.work_month,
        all_combinations.time_category,
        CASE
            WHEN
                all_combinations.time_category = 'Ordinary'
                THEN 'Total Worked Hours (Own)'
            WHEN
                all_combinations.time_category = 'Sick Leave'
                THEN 'Total Sick Leave Hours'
            WHEN all_combinations.time_category = 'Overtime' THEN 'Total Overtime Hours'
            ELSE 'Other Hours'
        END                              AS metric
    FROM all_combinations
    LEFT JOIN actual_records
        ON
            all_combinations.work_year = actual_records.work_year
            AND all_combinations.work_month = actual_records.work_month
            AND all_combinations.company_name = actual_records.company_name
            AND all_combinations.division_name = actual_records.division_name
            AND all_combinations.data_source_name = actual_records.data_source_name
            AND all_combinations.time_category = actual_records.time_category
    WHERE actual_records.record_id IS NULL
)

SELECT * FROM actual_records
UNION ALL
SELECT * FROM zero_hour_records
