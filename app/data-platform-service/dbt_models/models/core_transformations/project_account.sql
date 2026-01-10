WITH project_account AS (
    SELECT
        project_account_sk,
        tenant_id,
        client,
        company_id,
        company_name,
        division_id,
        division_name,
        department_id,
        department_name,
        project_id,
        project_name,
        project_identifier,
        ext_project_id,
        account,
        account_name,
        account_descr,
        year,
        period,
        period_value,
        amount_acc,
        amount_ytd,
        amount_period,
        data_source_id,
        is_cost,
        period_date
    FROM {{ ref('int__nrc_project_account')}}
)

SELECT * FROM project_account