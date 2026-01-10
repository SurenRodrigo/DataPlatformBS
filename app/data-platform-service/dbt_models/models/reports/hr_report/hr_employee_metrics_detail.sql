WITH monthly_data AS (
    SELECT
        active_company,
        active_division,
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

all_month_divisions AS (
    SELECT
        year,
        month,
        month_name,
        active_company,
        active_division
    FROM monthly_data
    GROUP BY
        year,
        month,
        month_name,
        active_company,
        active_division
),

all_metrics AS (
    SELECT 'Opening Balance' AS metric
    UNION ALL
    SELECT 'Closing Balance'
    UNION ALL
    SELECT 'Joiners'
    UNION ALL
    SELECT 'Leavers'
    UNION ALL
    SELECT 'Voluntary Leavers'
    UNION ALL
    SELECT 'Voluntary Leavers - Retired'
    UNION ALL
    SELECT 'Voluntary Leavers - Other'
    UNION ALL
    SELECT 'Involuntary Leavers'
    UNION ALL
    SELECT 'Women Employees'
    UNION ALL
    SELECT 'Men Employees'
    UNION ALL
    SELECT 'Women Employees - White Collar'
    UNION ALL
    SELECT 'Women Employees - Blue Collar'
    UNION ALL
    SELECT 'Men Employees - White Collar'
    UNION ALL
    SELECT 'Men Employees - Blue Collar'
    UNION ALL
    SELECT 'Apprentices'
    UNION ALL
    SELECT 'Temp Employees'
    UNION ALL
    SELECT 'Permanent Employees'
    UNION ALL
    SELECT 'Internal Moves'
),

all_combinations AS (
    SELECT
        month_div.year,
        month_div.month,
        month_div.month_name,
        month_div.active_company,
        month_div.active_division,
        metrics.metric
    FROM all_month_divisions AS month_div
    CROSS JOIN all_metrics AS metrics
),

-- Closing balance employees (active employees in current month)
closing_balance_employees AS (
    SELECT
        active_company,
        active_division,
        year,
        month,
        month_name,
        employee_id
    FROM (
        SELECT
            mon.active_company,
            mon.active_division,
            mon.year,
            mon.month,
            mon.month_name,
            mon.employee_id,
            ROW_NUMBER() OVER (
                PARTITION BY
                    mon.active_company,
                    mon.active_division,
                    mon.year,
                    mon.month,
                    mon.employee_id
                ORDER BY mon.date_key DESC
            ) AS row_num
        FROM monthly_data AS mon
        WHERE mon.is_active = TRUE
    ) AS dedup
    WHERE row_num = 1
),

-- Opening balance employees (active employees from previous month)
opening_balance_employees AS (
    SELECT
        curr.active_company,
        curr.active_division,
        curr.year,
        curr.month,
        curr.month_name,
        prev.employee_id
    FROM all_month_divisions AS curr
    INNER JOIN closing_balance_employees AS prev
        ON
            curr.active_company = prev.active_company
            AND curr.active_division = prev.active_division
            AND (curr.year * 12 + curr.month) = (prev.year * 12 + prev.month + 1)
),

joiner_employees AS (
    SELECT
        active_company,
        active_division,
        year,
        month,
        month_name,
        employee_id
    FROM monthly_data
    WHERE is_joiner = TRUE
    GROUP BY active_company, active_division, year, month, month_name, employee_id
),

leaver_employees AS (
    SELECT
        curr.active_company,
        curr.active_division,
        curr.year,
        curr.month,
        curr.month_name,
        prev.employee_id
    FROM (
        SELECT
            active_company,
            active_division,
            year,
            month,
            month_name
        FROM monthly_data
        GROUP BY active_company, active_division, year, month, month_name
    ) AS curr
    INNER JOIN (
        SELECT
            active_company,
            active_division,
            year,
            month,
            month_name,
            employee_id
        FROM monthly_data
        WHERE is_leaver = TRUE
        GROUP BY active_company, active_division, year, month, month_name, employee_id
    ) AS prev
        ON
            curr.active_company = prev.active_company
            AND curr.active_division = prev.active_division
            AND (curr.year * 12 + curr.month) = (prev.year * 12 + prev.month + 1)
),


voluntary_leaver_employees AS (
    SELECT
        curr.active_company,
        curr.active_division,
        curr.year,
        curr.month,
        curr.month_name,
        prev.employee_id
    FROM (
        SELECT
            active_company,
            active_division,
            year,
            month,
            month_name
        FROM monthly_data
        GROUP BY active_company, active_division, year, month, month_name
    ) AS curr
    INNER JOIN (
        SELECT
            active_company,
            active_division,
            year,
            month,
            month_name,
            employee_id
        FROM monthly_data
        WHERE is_voluntary_leaver = TRUE
        GROUP BY active_company, active_division, year, month, month_name, employee_id
    ) AS prev
        ON
            curr.active_company = prev.active_company
            AND curr.active_division = prev.active_division
            AND (curr.year * 12 + curr.month) = (prev.year * 12 + prev.month + 1)
),

-- Retired voluntary leavers
voluntary_leaver_retired_employees AS (
    SELECT
        curr.active_company,
        curr.active_division,
        curr.year,
        curr.month,
        curr.month_name,
        prev.employee_id
    FROM (
        SELECT
            active_company,
            active_division,
            year,
            month,
            month_name
        FROM monthly_data
        GROUP BY active_company, active_division, year, month, month_name
    ) AS curr
    INNER JOIN (
        SELECT
            active_company,
            active_division,
            year,
            month,
            month_name,
            employee_id
        FROM monthly_data
        WHERE
            is_voluntary_leaver = TRUE
            AND is_retired = TRUE
        GROUP BY active_company, active_division, year, month, month_name, employee_id
    ) AS prev
        ON
            curr.active_company = prev.active_company
            AND curr.active_division = prev.active_division
            AND (curr.year * 12 + curr.month) = (prev.year * 12 + prev.month + 1)
),

-- Voluntary leavers except retired
voluntary_leaver_other_employees AS (
    SELECT
        curr.active_company,
        curr.active_division,
        curr.year,
        curr.month,
        curr.month_name,
        prev.employee_id
    FROM (
        SELECT
            active_company,
            active_division,
            year,
            month,
            month_name
        FROM monthly_data
        GROUP BY active_company, active_division, year, month, month_name
    ) AS curr
    INNER JOIN (
        SELECT
            active_company,
            active_division,
            year,
            month,
            month_name,
            employee_id
        FROM monthly_data
        WHERE
            is_voluntary_leaver = TRUE
            AND is_retired = FALSE
        GROUP BY active_company, active_division, year, month, month_name, employee_id
    ) AS prev
        ON
            curr.active_company = prev.active_company
            AND curr.active_division = prev.active_division
            AND (curr.year * 12 + curr.month) = (prev.year * 12 + prev.month + 1)
),

-- Involuntary leaver employees (leavers minus voluntary leavers)
involuntary_leaver_employees AS (
    SELECT
        leaver.active_company,
        leaver.active_division,
        leaver.year,
        leaver.month,
        leaver.month_name,
        leaver.employee_id
    FROM leaver_employees AS leaver
    LEFT JOIN voluntary_leaver_employees AS voluntary
        ON
            leaver.active_company = voluntary.active_company
            AND leaver.active_division = voluntary.active_division
            AND leaver.year = voluntary.year
            AND leaver.month = voluntary.month
            AND leaver.employee_id = voluntary.employee_id
    WHERE voluntary.employee_id IS NULL
),

employee_last_active_day AS (
    SELECT
        active_company,
        active_division,
        year,
        month,
        month_name,
        employee_id,
        MAX(date_key) AS last_active_date
    FROM monthly_data
    WHERE is_active = TRUE
    GROUP BY active_company, active_division, year, month, month_name, employee_id
),

employee_final_division AS (
    SELECT
        employee_id,
        year,
        month,
        month_name,
        active_company,
        active_division,
        last_active_date
    FROM employee_last_active_day
    WHERE (employee_id, year, month, last_active_date) IN (
        SELECT
            employee_id,
            year,
            month,
            MAX(last_active_date) AS max_last_active_date
        FROM employee_last_active_day
        GROUP BY employee_id, year, month
    )
),

employee_demographics_base AS (
    SELECT
        employee_final.active_company,
        employee_final.active_division,
        employee_final.year,
        employee_final.month,
        employee_final.month_name,
        employee_final.employee_id,
        monthly_employee_data.is_female,
        monthly_employee_data.is_male,
        monthly_employee_data.is_white_collar,
        monthly_employee_data.is_blue_collar,
        monthly_employee_data.is_apprentice,
        monthly_employee_data.is_temp_employee
    FROM employee_final_division AS employee_final
    LEFT JOIN monthly_data AS monthly_employee_data
        ON
            employee_final.active_company = monthly_employee_data.active_company
            AND employee_final.active_division = monthly_employee_data.active_division
            AND employee_final.year = monthly_employee_data.year
            AND employee_final.month = monthly_employee_data.month
            AND employee_final.employee_id = monthly_employee_data.employee_id
            AND employee_final.last_active_date = monthly_employee_data.date_key
),

women_employees AS (
    SELECT
        active_company,
        active_division,
        year,
        month,
        month_name,
        employee_id
    FROM employee_demographics_base
    WHERE is_female = TRUE
),

white_collar_women_employees AS (
    SELECT
        active_company,
        active_division,
        year,
        month,
        month_name,
        employee_id
    FROM employee_demographics_base
    WHERE is_female = TRUE AND is_white_collar = TRUE
),

blue_collar_women_employees AS (
    SELECT
        active_company,
        active_division,
        year,
        month,
        month_name,
        employee_id
    FROM employee_demographics_base
    WHERE is_female = TRUE AND is_blue_collar = TRUE
),

men_employees AS (
    SELECT
        active_company,
        active_division,
        year,
        month,
        month_name,
        employee_id
    FROM employee_demographics_base
    WHERE is_male = TRUE
),

white_collar_men_employees AS (
    SELECT
        active_company,
        active_division,
        year,
        month,
        month_name,
        employee_id
    FROM employee_demographics_base
    WHERE is_male = TRUE AND is_white_collar = TRUE
),

blue_collar_men_employees AS (
    SELECT
        active_company,
        active_division,
        year,
        month,
        month_name,
        employee_id
    FROM employee_demographics_base
    WHERE is_male = TRUE AND is_blue_collar = TRUE
),

apprentice_employees AS (
    SELECT
        active_company,
        active_division,
        year,
        month,
        month_name,
        employee_id
    FROM employee_demographics_base
    WHERE is_apprentice = TRUE
),

temp_employees AS (
    SELECT
        active_company,
        active_division,
        year,
        month,
        month_name,
        employee_id
    FROM employee_demographics_base
    WHERE is_temp_employee = TRUE
),

permanent_employees AS (
    SELECT
        closing.active_company,
        closing.active_division,
        closing.year,
        closing.month,
        closing.month_name,
        closing.employee_id
    FROM closing_balance_employees AS closing
    LEFT JOIN apprentice_employees AS apprentice
        ON
            closing.active_company = apprentice.active_company
            AND closing.active_division = apprentice.active_division
            AND closing.year = apprentice.year
            AND closing.month = apprentice.month
            AND closing.employee_id = apprentice.employee_id
    LEFT JOIN temp_employees AS temp_emp
        ON
            closing.active_company = temp_emp.active_company
            AND closing.active_division = temp_emp.active_division
            AND closing.year = temp_emp.year
            AND closing.month = temp_emp.month
            AND closing.employee_id = temp_emp.employee_id
    WHERE
        apprentice.employee_id IS NULL
        AND temp_emp.employee_id IS NULL
),

internal_moves_employees AS (
    WITH last_day_data AS (
        SELECT
            employee_id,
            active_company,
            active_division,
            year,
            month,
            month_name,
            (
                DATE_TRUNC('month', date_key) + INTERVAL '1 month' - INTERVAL '1 day'
            )::DATE AS last_day_of_month
        FROM monthly_data
        WHERE
            date_key
            = (
                DATE_TRUNC('month', date_key) + INTERVAL '1 month' - INTERVAL '1 day'
            )::DATE
            AND is_active = TRUE
            AND active_division IS NOT NULL
        GROUP BY
            employee_id,
            active_company,
            active_division,
            year,
            month,
            month_name,
            last_day_of_month
    ),

    employee_division_change AS (
        SELECT
            curr.employee_id,
            curr.active_company  AS curr_company,
            prev.active_company  AS prev_company,
            curr.active_division AS curr_division,
            prev.active_division AS prev_division,
            curr.year,
            curr.month,
            curr.month_name
        FROM last_day_data AS curr
        INNER JOIN last_day_data AS prev
            ON
                curr.employee_id = prev.employee_id
                AND (curr.year * 12 + curr.month) = (prev.year * 12 + prev.month + 1)
        WHERE
            curr.active_division IS DISTINCT FROM prev.active_division
            AND curr.active_division IS NOT NULL
            AND prev.active_division IS NOT NULL
        GROUP BY
            curr.employee_id,
            curr.active_company,
            prev.active_company,
            curr.active_division,
            prev.active_division,
            curr.year,
            curr.month,
            curr.month_name
    )

    SELECT
        prev_company  AS active_company,
        prev_division AS active_division,
        year,
        month,
        month_name,
        employee_id,
        -1            AS count
    FROM employee_division_change

    UNION ALL

    SELECT
        curr_company  AS active_company,
        curr_division AS active_division,
        year,
        month,
        month_name,
        employee_id,
        1             AS count
    FROM employee_division_change
),

actual_metrics AS (
    SELECT
        year::TEXT,
        month,
        month_name,
        active_company    AS company,
        active_division   AS division,
        'Opening Balance' AS metric,
        employee_id,
        1                 AS count
    FROM opening_balance_employees

    UNION ALL

    SELECT
        year::TEXT,
        month,
        month_name,
        active_company    AS company,
        active_division   AS division,
        'Closing Balance' AS metric,
        employee_id,
        1                 AS count
    FROM closing_balance_employees

    UNION ALL

    SELECT
        year::TEXT,
        month,
        month_name,
        active_company  AS company,
        active_division AS division,
        'Joiners'       AS metric,
        employee_id,
        1               AS count
    FROM joiner_employees

    UNION ALL

    SELECT
        year::TEXT,
        month,
        month_name,
        active_company  AS company,
        active_division AS division,
        'Leavers'       AS metric,
        employee_id,
        1               AS count
    FROM leaver_employees

    UNION ALL

    SELECT
        year::TEXT,
        month,
        month_name,
        active_company      AS company,
        active_division     AS division,
        'Voluntary Leavers' AS metric,
        employee_id,
        1                   AS count
    FROM voluntary_leaver_employees

    UNION ALL

    SELECT
        year::TEXT,
        month,
        month_name,
        active_company                AS company,
        active_division               AS division,
        'Voluntary Leavers - Retired' AS metric,
        employee_id,
        1                             AS count
    FROM voluntary_leaver_retired_employees

    UNION ALL

    SELECT
        year::TEXT,
        month,
        month_name,
        active_company              AS company,
        active_division             AS division,
        'Voluntary Leavers - Other' AS metric,
        employee_id,
        1                           AS count
    FROM voluntary_leaver_other_employees

    UNION ALL

    SELECT
        year::TEXT,
        month,
        month_name,
        active_company        AS company,
        active_division       AS division,
        'Involuntary Leavers' AS metric,
        employee_id,
        1                     AS count
    FROM involuntary_leaver_employees

    UNION ALL

    SELECT
        year::TEXT,
        month,
        month_name,
        active_company    AS company,
        active_division   AS division,
        'Women Employees' AS metric,
        employee_id,
        1                 AS count
    FROM women_employees

    UNION ALL

    SELECT
        year::TEXT,
        month,
        month_name,
        active_company  AS company,
        active_division AS division,
        'Men Employees' AS metric,
        employee_id,
        1               AS count
    FROM men_employees

    UNION ALL

    SELECT
        year::TEXT,
        month,
        month_name,
        active_company                   AS company,
        active_division                  AS division,
        'Women Employees - White Collar' AS metric,
        employee_id,
        1                                AS count
    FROM white_collar_women_employees

    UNION ALL

    SELECT
        year::TEXT,
        month,
        month_name,
        active_company                  AS company,
        active_division                 AS division,
        'Women Employees - Blue Collar' AS metric,
        employee_id,
        1                               AS count
    FROM blue_collar_women_employees

    UNION ALL

    SELECT
        year::TEXT,
        month,
        month_name,
        active_company                 AS company,
        active_division                AS division,
        'Men Employees - White Collar' AS metric,
        employee_id,
        1                              AS count
    FROM white_collar_men_employees

    UNION ALL

    SELECT
        year::TEXT,
        month,
        month_name,
        active_company                AS company,
        active_division               AS division,
        'Men Employees - Blue Collar' AS metric,
        employee_id,
        1                             AS count
    FROM blue_collar_men_employees

    UNION ALL

    SELECT
        year::TEXT,
        month,
        month_name,
        active_company  AS company,
        active_division AS division,
        'Apprentices'   AS metric,
        employee_id,
        1               AS count
    FROM apprentice_employees

    UNION ALL

    SELECT
        year::TEXT,
        month,
        month_name,
        active_company   AS company,
        active_division  AS division,
        'Temp Employees' AS metric,
        employee_id,
        1                AS count
    FROM temp_employees

    UNION ALL

    SELECT
        year::TEXT,
        month,
        month_name,
        active_company        AS company,
        active_division       AS division,
        'Permanent Employees' AS metric,
        employee_id,
        1                     AS count
    FROM permanent_employees

    UNION ALL

    SELECT
        year::TEXT,
        month,
        month_name,
        active_company   AS company,
        active_division  AS division,
        'Internal Moves' AS metric,
        employee_id,
        count
    FROM internal_moves_employees
)

-- Final result
SELECT
    year,
    month,
    month_name,
    TO_CHAR(
        DATE(CONCAT(year::TEXT, '-', LPAD(month::TEXT, 2, '0'), '-01')), 'Mon YYYY'
    ) AS month_label,
    DATE(
        CONCAT(year::TEXT, '-', LPAD(month::TEXT, 2, '0'), '-01')
    ) AS month_key,
    company,
    division,
    metric,
    employee_id,
    count
FROM actual_metrics

UNION ALL

-- Adding zero records for missing combinations of division-month-metric
SELECT
    all_combinations.year::TEXT,
    all_combinations.month,
    all_combinations.month_name,
    TO_CHAR(
        DATE(
            CONCAT(
                all_combinations.year::TEXT,
                '-',
                LPAD(all_combinations.month::TEXT, 2, '0'),
                '-01'
            )
        ),
        'Mon YYYY'
    )                                AS month_label,
    DATE(
        CONCAT(
            all_combinations.year::TEXT,
            '-',
            LPAD(all_combinations.month::TEXT, 2, '0'),
            '-01'
        )
    )                                AS month_key,
    all_combinations.active_company  AS company,
    all_combinations.active_division AS division,
    all_combinations.metric,
    'No records'                     AS employee_id,
    0                                AS count
FROM all_combinations AS all_combinations
LEFT JOIN actual_metrics AS actual_metrics
    ON
        all_combinations.year = actual_metrics.year::INTEGER
        AND all_combinations.month = actual_metrics.month
        AND all_combinations.active_company = actual_metrics.company
        AND all_combinations.active_division = actual_metrics.division
        AND all_combinations.metric = actual_metrics.metric
WHERE actual_metrics.year IS NULL

ORDER BY year, month, company, division, metric, employee_id
