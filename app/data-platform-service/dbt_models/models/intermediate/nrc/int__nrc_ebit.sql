WITH company_mapping AS (
    SELECT
        ext_company_id,
        company_guid,
        data_source_id,
        tenant_id
    FROM {{ ref('company_data_source_seed')}}
),

ebit_data AS (
    SELECT
        client,
        period,
        project,
        year,
        month,
        month_name,
        period_date,
        acc_ebit,
        ytd_ebit,
        period_ebit,
        acc_revenue,
        period_revenue,
        ytd_revenue
    FROM {{ ref('stg__unit4_ebit') }}
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
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['ebit_data.client', 'ebit_data.period', 'ebit_data.project', 'ebit_data.period_date']) }} AS ebit_sk,
    company.tenant_id,
    org.company_id,
    org.company_name,
    org.division_id,
    org.division_name,
    org.department_id,
    org.department_name,
    project.id          AS project_id, 
    ebit_data.year,
    ebit_data.month,
    ebit_data.month_name,
    ebit_data.period    AS period_value,
    ebit_data.period_date,
    project.project_name,
    project.project_identifier,
    ebit_data.client,
    ebit_data.project   AS ext_project_id,
    ebit_data.acc_ebit,
    ebit_data.ytd_ebit,
    ebit_data.period_ebit,
    ebit_data.acc_revenue,
    ebit_data.period_revenue,
    ebit_data.ytd_revenue,
    org.data_source_id,
    org.data_source_name
FROM ebit_data
LEFT JOIN company_mapping AS company
    ON company.ext_company_id = ebit_data.client::TEXT
LEFT JOIN {{ ref('project') }} AS project
    ON project.ext_project_id = ebit_data.project
    AND project.company_id = company.company_guid
LEFT JOIN organizations AS org
    ON org.department_id = project.department_id
    AND org.data_source_id = company.data_source_id
    AND org.data_source_name = 'Unit4'
