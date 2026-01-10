SELECT
    NULLIF(
        clientid, ''
    )::INT               AS client,
    NULLIF(
        foeht, ''
    )                    AS division,
    NULLIF(
        department, ''
    )                    AS department,
    NULLIF(
        project, ''
    )::INT               AS project,
    (
        period::INT
    )::TEXT              AS period_value,
    (
        period::INT
    )                    AS period,
    LEFT(
        (period::INT)::TEXT, 4
    )::INT               AS year,
    RIGHT(
        (period::INT)::TEXT, 2
    )::INT               AS month,
    TO_CHAR(
        TO_DATE(RIGHT((period::INT)::TEXT, 2), 'MM'), 'MON'
    )                    AS month_name,
    (
        DATE_TRUNC(
            'month',
            TO_DATE(
                LEFT((period::INT)::TEXT, 4) || RIGHT((period::INT)::TEXT, 2), 'YYYYMM'
            )
        )
        + INTERVAL '1 month'
        - INTERVAL '1 day'
    )::DATE              AS period_date,
    amountacc::DECIMAL AS amount_acc,
    vo,
    -- DBT metadata
    dbt_scd_id,
    dbt_updated_at,
    dbt_valid_from,
    dbt_valid_to
FROM {{ ref('unit4_liquidity_project_snapshot') }}
WHERE
    dbt_valid_to IS NULL
    AND SUBSTRING(period::TEXT FROM 5 FOR 2)::INT < 13
    AND SUBSTRING(period::TEXT FROM 5 FOR 2)::INT > 0
