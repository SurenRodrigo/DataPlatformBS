WITH department_mapping AS (
    SELECT *
    FROM {{ ref('int__department_external_id_mapping') }}
    WHERE data_source_name = 'iCore'
),

-- Filter out problematic terminated records with NULL end dates
filtered_periods AS (
    SELECT DISTINCT
        employee_id,
        organisation_unit_id AS department_id,
        valid_from_date,
        valid_to_date,
        status_description_sl
    FROM {{ ref('stg__icore_employee_periodic') }}
    WHERE
        employee_id IS NOT NULL
        AND organisation_unit_id IS NOT NULL
        -- Exclude terminated records with NULL valid_to_date to prevent overlaps
        AND NOT (
            status_description_sl = 'Terminated'
            -- This represents NULL in the staging model
            AND valid_to_date = DATE '9999-12-31'
        )
        -- Additional data quality filters
        AND valid_from_date IS NOT NULL
        AND valid_from_date <= valid_to_date  -- Ensure valid date ranges
        AND valid_from_date != DATE '9999-12-31'  -- Exclude invalid start dates
),

sorted_periods AS (
    SELECT DISTINCT
        employee_id,
        department_id,
        valid_from_date,
        valid_to_date
    FROM filtered_periods
),

dept_change_analysis AS (
    SELECT
        *,
        CASE
            WHEN
                LAG(department_id) OVER (
                    PARTITION BY employee_id
                    ORDER BY valid_from_date, valid_to_date
                ) IS NULL
                OR LAG(department_id) OVER (
                    PARTITION BY employee_id
                    ORDER BY valid_from_date, valid_to_date
                ) != department_id
                THEN 1
            ELSE 0
        END AS is_dept_change
    FROM sorted_periods
),

dept_tenure_groups AS (
    SELECT
        *,
        SUM(is_dept_change) OVER (
            PARTITION BY employee_id
            ORDER BY valid_from_date, valid_to_date
            ROWS UNBOUNDED PRECEDING
        ) AS dept_tenure_id
    FROM dept_change_analysis
),

consolidated_tenures AS (
    SELECT
        employee_id,
        department_id,
        dept_tenure_id,
        MIN(valid_from_date) AS tenure_start,
        CASE
            WHEN MAX(CASE WHEN valid_to_date = DATE '9999-12-31' THEN 1 ELSE 0 END) = 1
                THEN NULL
            ELSE MAX(valid_to_date)
        END                  AS tenure_end,
        COUNT(*)             AS periods_consolidated
    FROM dept_tenure_groups
    GROUP BY employee_id, department_id, dept_tenure_id
),

-- Add next department start date for end date correction
tenures_with_next_start AS (
    SELECT
        *,
        LEAD(tenure_start) OVER (
            PARTITION BY employee_id
            ORDER BY tenure_start
        ) AS next_dept_start_date
    FROM consolidated_tenures
),

-- Correct the end dates based on business logic
corrected_tenures AS (
    SELECT
        employee_id,
        department_id,
        tenure_start,
        CASE
            -- If there's a next department and current tenure_end is NULL or after next start
            WHEN
                next_dept_start_date IS NOT NULL
                AND (tenure_end IS NULL OR tenure_end >= next_dept_start_date)
                THEN next_dept_start_date - INTERVAL '1 day'
            -- Keep original tenure_end if it's before next department start
            ELSE tenure_end
        END AS corrected_tenure_end,
        periods_consolidated
    FROM tenures_with_next_start
),

icore_employee_history AS (
    SELECT
        employee_id,
        department_id,
        tenure_start                                            AS valid_from_date,
        COALESCE(corrected_tenure_end::DATE, DATE '9999-12-31') AS valid_to_date,
        periods_consolidated
    FROM corrected_tenures
    ORDER BY employee_id, valid_from_date
),

final AS (
    SELECT
        emp_history.employee_id,
        emp_history.valid_from_date,
        emp_history.valid_to_date,
        dept_map.department_id,
        dept_map.department_name,
        dept_map.division_id,
        dept_map.division_name,
        dept_map.company_id,
        dept_map.company_name
    FROM icore_employee_history AS emp_history
    LEFT JOIN department_mapping AS dept_map
        ON emp_history.department_id = dept_map.external_id
)

SELECT *
FROM final
