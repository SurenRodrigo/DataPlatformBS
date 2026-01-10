SELECT
    NULLIF(projectId, '')                                       AS ext_project_id,
    year::INT                                                   AS year,
    month::INT                                                  AS month_number,
    TO_CHAR(MAKE_DATE(year::INT, month::INT, 1), 'Month') AS month_name,
    NULLIF(casetype, '')                                        AS variation_type_name,
    COALESCE(totalamount, 0)                                    AS total_amount,
    COALESCE(
        totalacceptedamount, 0
    )                                                             AS total_accepted_amount,
    COALESCE(
        totaldeclinedamount, 0
    )                                                             AS total_declined_amount,
    COALESCE(
        totalnothandledamount, 0
    )                                                             AS total_not_handled_amount,
    dbt_scd_id,
    dbt_updated_at,
    dbt_valid_from,
    dbt_valid_to
FROM {{ ref('endringer_amount_snapshot') }}
WHERE dbt_valid_to IS NULL
