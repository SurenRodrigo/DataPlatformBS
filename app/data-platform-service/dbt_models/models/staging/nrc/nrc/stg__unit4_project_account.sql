SELECT
    NULLIF(client, '')::INT    AS client,
    (period::INT)::TEXT        AS period,
    NULLIF(account, '')::INT   AS account,
    NULLIF(project, '')::INT   AS project,
    amountacc                AS amount_acc,
    amountytd                AS amount_ytd,
    amountperiod             AS amount_period,
    NULLIF(accountdescr, '') AS account_descr,
       (
        DATE_TRUNC(
            'month',
            TO_DATE(
                LEFT((period::INT)::TEXT, 4) || RIGHT((period::INT)::TEXT, 2), 'YYYYMM'
            )
        )
        + INTERVAL '1 month'
        - INTERVAL '1 day'
    )::DATE                     AS period_date,
    -- DBT metadata
    dbt_scd_id,
    dbt_updated_at,
    dbt_valid_from,
    dbt_valid_to
FROM {{ ref('unit4_project_account_snapshot') }}
WHERE dbt_valid_to IS NULL