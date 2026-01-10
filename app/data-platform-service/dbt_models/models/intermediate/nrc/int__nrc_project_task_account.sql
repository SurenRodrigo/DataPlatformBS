WITH company_mapping AS (
    SELECT
        ext_company_id,
        company_guid,
        data_source_id,
        tenant_id
    FROM {{ ref('company_data_source_seed')}}
),

project_task_accounts AS (
    SELECT
        project_id,
        project_task_id,
        project_task_name,
        period,
        client_id,
        amount_period,
        amount_acc,
        amount_ytd,
        amount_type
    FROM {{ ref('stg__unit4_project_task_account') }}
),

project_details AS (
    SELECT 
        {{ dbt_utils.generate_surrogate_key(['task_account.project_id', 'project_task_id', 'client_id', 'period', 'amount_type']) }} AS project_task_account_sk,
        department_mapping.tenant_id,
        department_mapping.tenant_name,
        task_account.project_id,
        task.project_task_sk AS project_task_sk,
        task_account.project_task_id AS ext_project_task_id,
        task_account.project_task_name,
        project.project_name,
        project.project_identifier,
        department_mapping.company_id,
        department_mapping.company_name,
        department_mapping.division_id,
        department_mapping.division_name,
        department_mapping.department_id,
        department_mapping.department_name,
        LEFT(period, 4)::INTEGER AS year,
        RIGHT(period, 2)::INTEGER AS period,
        task_account.period AS period_value,
        task_account.amount_period,
        task_account.amount_acc,
        task_account.amount_ytd,
        CASE 
            WHEN task_account.amount_type = 'I' THEN 'Income'
            WHEN task_account.amount_type = 'K' THEN 'Cost'
            ELSE 'Unknown'
        END AS amount_type,
        department_mapping.data_source_id,
        department_mapping.data_source_name
    FROM project_task_accounts task_account 
    LEFT JOIN company_mapping AS company
        ON task_account.client_id::TEXT = company.ext_company_id
        AND company.data_source_id = 7
    LEFT JOIN {{ ref('project') }} project
        ON task_account.project_id = project.ext_project_id
        AND company.company_guid = project.company_id
    LEFT JOIN {{ ref('int__department_external_id_mapping') }} department_mapping
        ON project.department_id = department_mapping.department_id
        AND department_mapping.data_source_name = 'Unit4'
    LEFT JOIN {{ ref('int__nrc_project_task') }} task
        ON task_account.project_id = task.project_id
        AND task_account.project_task_id = task.ext_project_task_id    
)

SELECT
    project_task_account_sk,
    tenant_id,
    tenant_name,
    project_id,
    project_task_sk,
    ext_project_task_id,
    project_task_name,
    project_name,
    project_identifier,
    company_id,
    company_name,
    division_id,
    division_name,
    department_id,
    department_name,
    year,
    period,
    period_value,
    amount_period,
    amount_acc,
    amount_ytd,
    amount_type,
    data_source_id,
    data_source_name
FROM project_details
