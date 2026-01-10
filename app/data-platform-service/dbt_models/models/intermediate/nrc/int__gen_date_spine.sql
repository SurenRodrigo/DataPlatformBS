WITH date_spine AS (
    SELECT
        generate_series(
            '2024-01-01'::DATE,
            CURRENT_DATE,  
            '1 day'::INTERVAL
        )::DATE AS date_day
),
gen_dim_calendar AS (
    SELECT
        date_day AS date_key,
        date_day,
        EXTRACT(YEAR FROM date_day) AS year,
        EXTRACT(MONTH FROM date_day) AS month,
        EXTRACT(DAY FROM date_day) AS day,
        EXTRACT(DOW FROM date_day) AS day_of_week, -- 0=Sunday, 6=Saturday
        CASE
            WHEN EXTRACT(DOW FROM date_day) IN (0, 6) THEN TRUE
            ELSE FALSE
        END AS is_weekend,
        EXTRACT(WEEK FROM date_day) AS week_of_year,
        EXTRACT(QUARTER FROM date_day) AS quarter,
        TO_CHAR(date_day, 'Month') AS month_name,
        TO_CHAR(date_day, 'Dy') AS day_name,
        CASE
            WHEN EXTRACT(MONTH FROM date_day) BETWEEN 1 AND 3 THEN 1
            WHEN EXTRACT(MONTH FROM date_day) BETWEEN 4 AND 6 THEN 2
            WHEN EXTRACT(MONTH FROM date_day) BETWEEN 7 AND 9 THEN 3
            ELSE 4
        END AS fiscal_quarter,
        CASE
            WHEN EXTRACT(MONTH FROM date_day) >= 7
            THEN EXTRACT(YEAR FROM date_day) + 1
            ELSE EXTRACT(YEAR FROM date_day)
        END AS fiscal_year
    FROM date_spine
)

SELECT * FROM gen_dim_calendar