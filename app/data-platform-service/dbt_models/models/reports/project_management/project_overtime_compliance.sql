WITH overtime_daily AS (
    SELECT
        time_record.ext_employee_id AS employee_id,
        time_record.company_id,
        time_record.project_id,
        time_record.project_identifier,
        (time_record.date)::date    AS work_date,
        SUM(time_record.hours)      AS overtime_hours
    FROM {{ ref('time_record') }} AS time_record
    WHERE
        time_record.is_overtime = TRUE
    GROUP BY
        time_record.ext_employee_id,
        time_record.company_id,
        time_record.project_id,
        time_record.project_identifier,
        (time_record.date)::date
),

company_limits AS (
    SELECT
        1   AS company_id,
        50  AS limit_28d,
        200 AS limit_year
    UNION ALL
    SELECT
        2   AS company_id,
        40  AS limit_28d,
        300 AS limit_year
),

daily_with_limits AS (
    SELECT
        overtime_daily.*,
        company_limits.limit_28d,
        company_limits.limit_year
    FROM overtime_daily AS overtime_daily
    LEFT JOIN
        company_limits AS company_limits
        ON overtime_daily.company_id = company_limits.company_id
),

rolling_28_days AS (
    SELECT
        daily_with_limits.employee_id,
        daily_with_limits.project_id,
        daily_with_limits.project_identifier,
        daily_with_limits.work_date,
        daily_with_limits.limit_28d                             AS overtime_limit,
        SUM(daily_with_limits.overtime_hours) OVER (
            PARTITION BY daily_with_limits.employee_id, daily_with_limits.project_id
            ORDER BY daily_with_limits.work_date
            RANGE BETWEEN INTERVAL '27 day' PRECEDING AND CURRENT ROW
        )                                                       AS cumulative_hours,
        (daily_with_limits.work_date - interval '27 day')::date AS window_start_date,
        daily_with_limits.work_date                             AS window_end_date
    FROM daily_with_limits AS daily_with_limits
),

calendar_year AS (
    SELECT
        daily_with_limits.employee_id,
        daily_with_limits.project_id,
        daily_with_limits.project_identifier,
        daily_with_limits.work_date,
        daily_with_limits.limit_year
            AS overtime_limit,
        SUM(daily_with_limits.overtime_hours)
            OVER (
                PARTITION BY
                    daily_with_limits.employee_id,
                    daily_with_limits.project_id,
                    DATE_TRUNC('year', daily_with_limits.work_date)
                ORDER BY daily_with_limits.work_date
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            )
            AS cumulative_hours,
        DATE_TRUNC('year', daily_with_limits.work_date)::date
            AS window_start_date,
        (
            DATE_TRUNC('year', daily_with_limits.work_date)
            + interval '1 year'
            - interval '1 day'
        )::date
            AS window_end_date
    FROM daily_with_limits AS daily_with_limits
)

SELECT
    'Rolling 4-Week'
        AS compliance_type,
    rolling_28_days.employee_id,
    rolling_28_days.project_identifier,
    rolling_28_days.window_start_date::date
        AS window_start_date,
    rolling_28_days.window_end_date::date
        AS window_end_date,
    rolling_28_days.work_date
        AS date,
    to_char(rolling_28_days.work_date, 'YYYY')
        AS year,
    to_char(rolling_28_days.work_date, 'FMMonth')
        AS month_name,
    rolling_28_days.cumulative_hours
        AS overtime_hours,
    rolling_28_days.overtime_limit,
    COALESCE(rolling_28_days.cumulative_hours > rolling_28_days.overtime_limit, FALSE)
        AS exceeded_limit
FROM rolling_28_days AS rolling_28_days
WHERE rolling_28_days.cumulative_hours > rolling_28_days.overtime_limit
UNION ALL
SELECT
    'Annual'
        AS compliance_type,
    calendar_year.employee_id,
    calendar_year.project_identifier,
    calendar_year.window_start_date::date
        AS window_start_date,
    calendar_year.window_end_date::date
        AS window_end_date,
    calendar_year.work_date
        AS date,
    to_char(calendar_year.work_date, 'YYYY')
        AS year,
    to_char(calendar_year.work_date, 'FMMonth')
        AS month_name,
    calendar_year.cumulative_hours
        AS overtime_hours,
    calendar_year.overtime_limit,
    COALESCE(calendar_year.cumulative_hours > calendar_year.overtime_limit, FALSE)
        AS exceeded_limit
FROM calendar_year AS calendar_year
WHERE calendar_year.cumulative_hours > calendar_year.overtime_limit
ORDER BY employee_id, window_end_date