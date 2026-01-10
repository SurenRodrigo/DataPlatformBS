SELECT
    NULLIF(project, '')::INT   AS project_id,
    NULLIF(prodkode, '')       AS project_task_id,
    (period::INT)::TEXT        AS period,
    NULLIF(client, '')::INT    AS client_id,
    NULLIF(prodkodenavn, '')   AS project_task_name,
    amountperiod               AS amount_period,
    amountacc                  AS amount_acc,
    amountytd                  AS amount_ytd,
    NULLIF(kode, '')           AS amount_type,
    dbt_scd_id,
    dbt_updated_at,
    dbt_valid_from,
    dbt_valid_to
FROM {{ ref('unit4_project_task_account_snapshot') }}
WHERE dbt_valid_to IS NULL
