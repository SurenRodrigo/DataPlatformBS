WITH vacancies AS (
    SELECT
        department_vacancy_id,
        published_at,
        last_application_date,
        division_id,
        organizational_unit_id AS department_id,
        is_removed,
        date_withdrawn
    FROM {{ ref('department_vacancies') }}
),

vacancies_with_periods AS (
    SELECT
        department_vacancy_id,
        published_at,
        last_application_date,
        division_id,
        department_id,
        is_removed,
        date_withdrawn,
        -- Calculate effective end date (whichever comes first: closing date or withdrawn date)
        CASE
            WHEN is_removed = TRUE AND last_application_date IS NOT NULL
                THEN
                    LEAST(date_withdrawn, last_application_date)
            WHEN is_removed = TRUE
                THEN
                    date_withdrawn
            WHEN last_application_date IS NOT NULL
                THEN
                    last_application_date -- Still open (no end date)
        END AS effective_end_date
    FROM vacancies
),

-- Generate a row for each month that a vacancy is active
monthly_vacancies AS (
    SELECT
        vacancy_data.department_vacancy_id,
        vacancy_data.published_at,
        vacancy_data.last_application_date,
        vacancy_data.division_id,
        vacancy_data.department_id,
        vacancy_data.is_removed,
        vacancy_data.date_withdrawn,
        vacancy_data.effective_end_date,
        -- Generate month boundaries for each month the vacancy is active
        DATE_TRUNC(
            'month', month_series.month_date
        )::DATE AS month_start,
        (
            DATE_TRUNC('month', month_series.month_date)
            + INTERVAL '1 month'
            - INTERVAL '1 day'
        )::DATE AS month_end,
        EXTRACT(
            YEAR FROM month_series.month_date
        )::TEXT AS year,
        EXTRACT(
            MONTH FROM month_series.month_date
        )       AS month,
        TO_CHAR(
            TO_DATE(EXTRACT(MONTH FROM month_series.month_date)::TEXT, 'MM'), 'Month'
        )       AS month_name,
        TO_CHAR(
            TO_DATE(
                EXTRACT(YEAR FROM month_series.month_date)::TEXT
                || '-'
                || LPAD(EXTRACT(MONTH FROM month_series.month_date)::TEXT, 2, '0')
                || '-01',
                'YYYY-MM-DD'
            ),
            'Mon YYYY'
        )       AS month_label,
        TO_DATE(
            CONCAT(
                EXTRACT(YEAR FROM month_series.month_date)::TEXT,
                '-',
                LPAD(EXTRACT(MONTH FROM month_series.month_date)::TEXT, 2, '0'),
                '-01'
            ),
            'YYYY-MM-DD'
        )       AS month_key
    FROM vacancies_with_periods AS vacancy_data
    CROSS JOIN (
        -- Generate a series of months from the earliest published date to current date
        SELECT
            GENERATE_SERIES(
                DATE_TRUNC('month', MIN(published_at))::DATE,
                DATE_TRUNC('month', CURRENT_DATE)::DATE,
                INTERVAL '1 month'
            )::DATE AS month_date
        FROM vacancies_with_periods
    ) AS month_series
    WHERE
        -- Vacancy was published on or before the end of this month
        vacancy_data.published_at
        <= (
            DATE_TRUNC('month', month_series.month_date)
            + INTERVAL '1 month'
            - INTERVAL '1 day'
        )::DATE
        AND
        -- Vacancy's effective end date is on or after the start of this month (or still open)
        (
            vacancy_data.effective_end_date IS NULL
            OR vacancy_data.effective_end_date
            >= DATE_TRUNC('month', month_series.month_date)::DATE
        )
),

department_mapping AS (
    SELECT
        company_id,
        company_name,
        division_id,
        division_name,
        department_id,
        department_name
    FROM {{ ref('int__department_denormalized') }}
),

vacancy_counts_by_department AS (
    SELECT
        division_id,
        department_id,
        year,
        month,
        month_name,
        month_label,
        month_key,
        COUNT(
            DISTINCT department_vacancy_id
        ) AS open_vacancy_count,
        COUNT(
            DISTINCT CASE WHEN is_removed = FALSE THEN department_vacancy_id END
        ) AS currently_active_vacancy_count,
        COUNT(
            DISTINCT CASE WHEN is_removed = TRUE THEN department_vacancy_id END
        ) AS withdrawn_during_period_count,
        MIN(
            published_at
        ) AS earliest_posting_date,
        MAX(
            published_at
        ) AS latest_posting_date
    FROM monthly_vacancies
    GROUP BY division_id, department_id, year, month, month_name, month_label, month_key
)

SELECT
    vacancy_counts.year,
    vacancy_counts.month,
    vacancy_counts.month_name,
    vacancy_counts.month_label,
    vacancy_counts.month_key,
    vacancy_counts.division_id,
    vacancy_counts.department_id,
    department_info.company_id,
    COALESCE(department_info.division_name, '')   AS division_name,
    COALESCE(department_info.department_name, '') AS department_name,
    COALESCE(department_info.company_name, '')    AS company_name,
    vacancy_counts.open_vacancy_count,
    vacancy_counts.currently_active_vacancy_count,
    vacancy_counts.withdrawn_during_period_count,
    vacancy_counts.earliest_posting_date,
    vacancy_counts.latest_posting_date
FROM vacancy_counts_by_department AS vacancy_counts
LEFT JOIN
    department_mapping AS department_info
    ON vacancy_counts.department_id = department_info.department_id
