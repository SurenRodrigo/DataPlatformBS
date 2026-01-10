-- depends_on: {{ ref('unit4_ebit_snapshot') }}

SELECT
    NULLIF(
        clientid, ''
    )::INT       AS client,
    (
        period::INT
    )::TEXT      AS period,
    LEFT(
        (period::INT)::TEXT, 4
    )::INT       AS year,
    RIGHT(
        (period::INT)::TEXT, 2
    )::INT       AS month,
    TO_CHAR(
        TO_DATE(RIGHT((period::INT)::TEXT, 2), 'MM'), 'MON'
    )            AS month_name,
    (
        DATE_TRUNC(
            'month',
            TO_DATE(
                LEFT((period::INT)::TEXT, 4) || RIGHT((period::INT)::TEXT, 2), 'YYYYMM'
            )
        )
        + INTERVAL '1 month'
        - INTERVAL '1 day'
    )::DATE      AS period_date,
    NULLIF(
        projectname, ''
    )            AS project_name,
    NULLIF(
        project, ''
    )::INT       AS project,
    accebit      AS acc_ebit,
    ytdebit      AS ytd_ebit,
    periodebit   AS period_ebit,
    accRevenue   AS acc_revenue,
    periodRevenue AS period_revenue,
    ytdRevenue   AS ytd_revenue,
    -- DBT metadata
    dbt_scd_id,
    dbt_updated_at,
    dbt_valid_from,
    dbt_valid_to
FROM {{ ref('unit4_ebit_snapshot') }}
WHERE
    dbt_valid_to IS NULL
    AND SUBSTRING(period::TEXT FROM 5 FOR 2)::INT < 13
    AND SUBSTRING(period::TEXT FROM 5 FOR 2)::INT > 0
    AND projectname != 'Dummy'
