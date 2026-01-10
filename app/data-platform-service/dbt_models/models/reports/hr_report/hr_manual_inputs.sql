WITH external_inputs AS (
    SELECT
        department_id,
        year,
        month,
        rented_workers_hours_count,
        subcontractor_hours_count,
        rented_personal_count,
        last_modified
    FROM {{ ref('int__nrc_external_inputs') }}
),

department_mapping AS (
    SELECT
        department_id,
        department_name,
        division_name,
        company_name
    FROM {{ ref('int__department_denormalized') }}
),

all_departments AS (
    SELECT
        dept_mapping.company_name,
        dept_mapping.division_name,
        dept_mapping.department_name
    FROM external_inputs AS ext_inputs
    LEFT JOIN
        department_mapping AS dept_mapping
        ON ext_inputs.department_id = dept_mapping.department_id
    GROUP BY
        dept_mapping.company_name,
        dept_mapping.division_name,
        dept_mapping.department_name
),

all_year_months AS (
    SELECT
        year,
        month
    FROM {{ ref('int__gen_date_spine') }}
    GROUP BY year, month
),

all_metrics AS (
    SELECT 'Rented Workers Total Hours' AS metric
    UNION ALL
    SELECT 'Subcontractor Total Hours'
    UNION ALL
    SELECT 'Rented Personal Count'
),

all_combinations AS (
    SELECT
        year_month.year,
        year_month.month,
        dept_data.company_name,
        dept_data.division_name,
        dept_data.department_name,
        metric_data.metric
    FROM all_departments AS dept_data
    CROSS JOIN all_year_months AS year_month
    CROSS JOIN all_metrics AS metric_data
),

pivoted_data AS (
    SELECT
        department_id,
        year,
        month,
        last_modified,
        'Rented Workers Total Hours' AS metric,
        rented_workers_hours_count   AS value
    FROM external_inputs

    UNION ALL

    SELECT
        department_id,
        year,
        month,
        last_modified,
        'Subcontractor Total Hours' AS metric,
        subcontractor_hours_count   AS value
    FROM external_inputs

    UNION ALL

    SELECT
        department_id,
        year,
        month,
        last_modified,
        'Rented Personal Count' AS metric,
        rented_personal_count   AS value
    FROM external_inputs
),

actual_data AS (
    SELECT
        dept_mapping.company_name,
        dept_mapping.division_name,
        dept_mapping.department_name,
        (pivoted_data.year)::TEXT                                 AS year,
        pivoted_data.month,
        TO_CHAR(TO_DATE(pivoted_data.month::TEXT, 'MM'), 'Month') AS month_name,
        pivoted_data.metric,
        pivoted_data.value,
        pivoted_data.last_modified
    FROM pivoted_data
    LEFT JOIN department_mapping AS dept_mapping
        ON pivoted_data.department_id = dept_mapping.department_id
)

-- Final result
SELECT
    company_name,
    division_name,
    department_name,
    year,
    month,
    month_name,
    TO_CHAR(
        TO_DATE(year || '-' || month || '-01', 'YYYY-MM-DD'),
        'Mon YYYY'
    ) AS month_label,
    TO_DATE(
        CONCAT(year, '-', month, '-01'), 'YYYY-MM-DD'
    ) AS month_key,
    metric,
    value,
    last_modified::TEXT
FROM actual_data

UNION ALL

-- Adding zero records for missing combinations of division-month-metric
SELECT
    combinations.company_name,
    combinations.division_name,
    combinations.department_name,
    combinations.year::TEXT,
    combinations.month,
    TO_CHAR(TO_DATE(combinations.month::TEXT, 'MM'), 'Month') AS month_name,
    TO_CHAR(
        TO_DATE(combinations.year || '-' || combinations.month || '-01', 'YYYY-MM-DD'),
        'Mon YYYY'
    )                                                         AS month_label,
    TO_DATE(
        CONCAT(combinations.year, '-', combinations.month, '-01'), 'YYYY-MM-DD'
    )                                                         AS month_key,
    combinations.metric,
    0                                                         AS value,
    'No Records'                                              AS last_modified
FROM all_combinations AS combinations
LEFT JOIN actual_data AS actual
    ON
        combinations.year = actual.year::INTEGER
        AND combinations.month = actual.month
        AND combinations.company_name = actual.company_name
        AND combinations.division_name = actual.division_name
        AND combinations.department_name = actual.department_name
        AND combinations.metric = actual.metric
WHERE actual.year IS NULL

ORDER BY company_name, division_name, department_name, year, month, metric
