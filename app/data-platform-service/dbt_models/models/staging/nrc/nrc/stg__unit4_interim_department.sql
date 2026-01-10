SELECT
    NULLIF(
        clientid, ''
    )::INT       AS client,
    NULLIF(
        project, ''
    )            AS project,
    NULLIF(
        department, ''
    )            AS department,
    vo,
    NULLIF(
        foeht, ''
    )            AS division,
    LEFT(
        (period::INT)::TEXT, 4
    )::INT       AS year,
    SUBSTRING(
        period::TEXT FROM 5 FOR 2
    )::INT       AS month,
    TO_CHAR(
        TO_DATE(SUBSTRING(period::TEXT FROM 5 FOR 2), 'MM'),
        'MON'
    )            AS month_name,
    (
        period::INT
    )::TEXT      AS period_value,
    (
        period::INT
    )            AS period,
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
    amountacc    AS amount_acc,
    amountyear   AS amount_year,
    amounttype   AS amount_type,
    CASE
        WHEN amounttype = '05' THEN 'interim_revenue'
        WHEN amounttype = '12' THEN 'interim_cost'
    END          AS amount_type_name,
    description,
    dbt_scd_id,
    dbt_updated_at,
    dbt_valid_from,
    dbt_valid_to
FROM {{ ref('unit4_interim_department_snapshot') }}
WHERE
    dbt_valid_to IS NULL
    AND SUBSTRING(period::TEXT FROM 5 FOR 2)::INT < 13
    AND SUBSTRING(period::TEXT FROM 5 FOR 2)::INT > 0
