WITH company_mapping AS (
    SELECT
        ext_company_id,
        company_guid,
        data_source_id,
        tenant_id
    FROM {{ ref('company_data_source_seed')}}
),

project_account AS (
    SELECT
        client,
        period,
        account,
        project,
        amount_acc,
        amount_ytd,
        amount_period,
        account_descr,
        period_date
    FROM {{ ref('stg__unit4_project_account') }}
),

organizations AS (
    SELECT
        department_id,
        department_name,
        division_id,
        division_name,
        company_id,
        company_name,
        data_source_id,
        data_source_name
    FROM {{ ref('int__department_external_id_mapping') }}
),

cost_accounts AS (
    SELECT
        account_number
    FROM {{ ref('cost_accounts_seed') }}
)
SELECT
    {{ dbt_utils.generate_surrogate_key(['project_account.client', 'project_account.period', 'project_account.project', 'project_account.account']) }} AS project_account_sk,
    company.tenant_id,
    org.company_id,
    org.company_name,
    org.division_id,
    org.division_name,
    org.department_id,
    org.department_name,
    project.id AS project_id, 
    LEFT(period, 4)::INT AS year,
    RIGHT(period, 2)::INT AS period,
    project.project_name,
    project.project_identifier,
    project_account.client,
    project_account.period AS period_value,
    project_account.account,
    (project_account.account || ' - ' || project_account.account_descr) AS account_name,
    project_account.project AS ext_project_id,
    project_account.amount_acc,
    project_account.amount_ytd,
    project_account.amount_period,
    project_account.account_descr,
    company.data_source_id,
      CASE 
        WHEN cost_accounts.account_number IS NOT NULL THEN TRUE 
        ELSE FALSE 
    END AS is_cost,
    project_account.period_date
FROM project_account
LEFT JOIN company_mapping AS company
    ON company.ext_company_id = project_account.client::TEXT
LEFT JOIN {{ ref('project') }} AS project
    ON project.ext_project_id = project_account.project
    AND project.company_id = company.company_guid
LEFT JOIN organizations AS org
    ON org.department_id = project.department_id
    AND org.data_source_id = company.data_source_id
    AND org.data_source_name = 'Unit4'
LEFT JOIN cost_accounts
    ON cost_accounts.account_number = project_account.account
