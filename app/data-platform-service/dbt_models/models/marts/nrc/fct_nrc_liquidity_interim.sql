WITH company_mapping AS (
    SELECT
        ext_company.ext_company_id,
        ext_company.company_guid,
        ext_company.data_source_id,
        company.company_name,
        data_source.name AS data_source_name
    FROM {{ ref('company_data_source_seed')}} AS ext_company
    LEFT JOIN {{ ref('company_seed')}} AS company
        ON company.id = ext_company.company_guid
        AND ext_company.data_source_id = 7
    LEFT JOIN {{ ref('data_source_seed')}} data_source
        ON data_source.id = ext_company.data_source_id
),

division_mapping AS (
    SELECT
        external_id,
        company_id,
        company_name,
        division_id,
        division_name,
        data_source_name
    FROM {{ ref('int__division_external_id_mapping')}}
    WHERE data_source_name = 'Unit4'
),

department_mapping AS (
    SELECT
        external_id,
        company_id,
        company_name,
        division_id,
        division_name,
        department_id,
        department_name,
        data_source_name
    FROM {{ ref('int__department_external_id_mapping')}}
    WHERE data_source_name = 'Unit4'
),

project_mapping AS (
    SELECT
        project_number,
        project_identifier,
        company_id,
        company_name,
        division_id,
        division_name,
        department_id,
        department_name,
        data_source_name
    FROM {{ ref('project')}}
    WHERE data_source_name = 'Unit4'
),

liquidity_company AS (
    SELECT
        'company' AS org_hierarchy_level,
        company_mapping.company_guid AS company_id,
        company_mapping.company_name,
        NULL::INT AS division_id,
        NULL AS division_name,
        NULL::INT AS department_id,
        NULL AS department_name,
        NULL::INT AS project_number,
        NULL AS project_identifier,
        liquidity.period_value,
        liquidity.period,
        liquidity.period_date,
        liquidity.year,
        liquidity.month,
        liquidity.month_name,
        liquidity.vo,
        'liquidity' AS amount_type,
        liquidity.amount_acc,
        NULL::DECIMAL AS amount_year,
        company_mapping.data_source_name
    FROM {{ ref('stg__unit4_liquidity_company') }} AS liquidity
    LEFT JOIN company_mapping
        ON liquidity.client::TEXT = company_mapping.ext_company_id
),

liquidity_division AS (
    SELECT
        'division' AS org_hierarchy_level,
        division_mapping.company_id,
        division_mapping.company_name,
        division_mapping.division_id,
        division_mapping.division_name,
        NULL::INT AS department_id,
        NULL AS department_name,
        NULL::INT AS project_number,
        NULL AS project_identifier,
        liquidity.period_value,
        liquidity.period,
        liquidity.period_date,
        liquidity.year,
        liquidity.month,
        liquidity.month_name,
        liquidity.vo,
        'liquidity' AS amount_type,
        liquidity.amount_acc,
        NULL::DECIMAL AS amount_year,
        division_mapping.data_source_name
    FROM {{ ref('stg__unit4_liquidity_division') }} liquidity
    LEFT JOIN division_mapping
        ON liquidity.division = division_mapping.external_id
),

liquidity_department AS (
    SELECT
        'department' AS org_hierarchy_level,
        department_mapping.company_id,
        department_mapping.company_name,
        department_mapping.division_id,
        department_mapping.division_name,
        department_mapping.department_id,
        department_mapping.department_name,
        NULL::INT AS project_number,
        NULL AS project_identifier,
        liquidity.period_value,
        liquidity.period,
        liquidity.period_date,
        liquidity.year,
        liquidity.month,
        liquidity.month_name,
        liquidity.vo,
        'liquidity' AS amount_type,
        liquidity.amount_acc,
        NULL::DECIMAL AS amount_year,
        department_mapping.data_source_name
    FROM {{ ref('stg__unit4_liquidity_department') }} liquidity
    LEFT JOIN department_mapping
        ON liquidity.department = department_mapping.external_id
),

liquidity_project AS (
    SELECT
        'project' AS org_hierarchy_level,
        project_mapping.company_id,
        project_mapping.company_name,
        project_mapping.division_id,
        project_mapping.division_name,
        project_mapping.department_id,
        project_mapping.department_name,
        project_mapping.project_number,
        project_mapping.project_identifier,
        liquidity.period_value,
        liquidity.period,
        liquidity.period_date,
        liquidity.year,
        liquidity.month,
        liquidity.month_name,
        liquidity.vo,
        'liquidity' AS amount_type,
        liquidity.amount_acc,
        NULL::DECIMAL AS amount_year,
        project_mapping.data_source_name
    FROM {{ ref('stg__unit4_liquidity_project') }} liquidity
    LEFT JOIN company_mapping
        ON liquidity.client::TEXT = company_mapping.ext_company_id
    LEFT JOIN project_mapping
        ON company_mapping.data_source_name = project_mapping.data_source_name
        AND liquidity.project = project_mapping.project_number
        AND project_mapping.company_id = company_mapping.company_guid
),

interim_company AS (
    SELECT
        'company' AS org_hierarchy_level,
        company_mapping.company_guid AS company_id,
        company_mapping.company_name,
        NULL::INT AS division_id,
        NULL AS division_name,
        NULL::INT AS department_id,
        NULL AS department_name,
        NULL::INT AS project_number,
        NULL AS project_identifier,
        interim.period_value,
        interim.period,
        interim.period_date,
        interim.year,
        interim.month,
        interim.month_name,
        interim.vo,
        interim.amount_type_name,
        interim.amount_acc,
        interim.amount_year,
        company_mapping.data_source_name
    FROM {{ ref('stg__unit4_interim_company') }} AS interim
    LEFT JOIN company_mapping
        ON interim.client::TEXT = company_mapping.ext_company_id
),

interim_division AS (
    SELECT
        'division' AS org_hierarchy_level,
        division_mapping.company_id,
        division_mapping.company_name,
        division_mapping.division_id,
        division_mapping.division_name,
        NULL::INT AS department_id,
        NULL AS department_name,
        NULL::INT AS project_number,
        NULL AS project_identifier,
        interim.period_value,
        interim.period,
        interim.period_date,
        interim.year,
        interim.month,
        interim.month_name,
        interim.vo,
        interim.amount_type_name,
        interim.amount_acc,
        interim.amount_year,
        division_mapping.data_source_name
    FROM {{ ref('stg__unit4_interim_division') }} interim
    LEFT JOIN division_mapping
        ON interim.division = division_mapping.external_id
),

interim_department AS (
    SELECT
        'department' AS org_hierarchy_level,
        department_mapping.company_id,
        department_mapping.company_name,
        department_mapping.division_id,
        department_mapping.division_name,
        department_mapping.department_id,
        department_mapping.department_name,
        NULL::INT AS project_number,
        NULL AS project_identifier,
        interim.period_value,
        interim.period,
        interim.period_date,
        interim.year,
        interim.month,
        interim.month_name,
        interim.vo,
        interim.amount_type_name,
        interim.amount_acc,
        interim.amount_year,
        department_mapping.data_source_name
    FROM {{ ref('stg__unit4_interim_department') }} interim
    LEFT JOIN department_mapping
        ON interim.department = department_mapping.external_id
),

interim_project AS (
    SELECT
        'project' AS org_hierarchy_level,
        project_mapping.company_id,
        project_mapping.company_name,
        project_mapping.division_id,
        project_mapping.division_name,
        project_mapping.department_id,
        project_mapping.department_name,
        project_mapping.project_number,
        project_mapping.project_identifier,
        interim.period_value,
        interim.period,
        interim.period_date,
        interim.year,
        interim.month,
        interim.month_name,
        interim.vo,
        interim.amount_type_name,
        interim.amount_acc,
        interim.amount_year,
        project_mapping.data_source_name
    FROM {{ ref('stg__unit4_interim_project') }} interim
    LEFT JOIN company_mapping
        ON interim.client::TEXT = company_mapping.ext_company_id
    LEFT JOIN project_mapping
        ON company_mapping.data_source_name = project_mapping.data_source_name
        AND interim.project = project_mapping.project_number
        AND project_mapping.company_id = company_mapping.company_guid
),

combined_data AS (
    SELECT * from liquidity_company
    UNION ALL
    SELECT * from liquidity_division
    UNION ALL
    SELECT * from liquidity_department
    UNION ALL
    SELECT * from liquidity_project
    UNION ALL
    SELECT * from interim_company
    UNION ALL
    SELECT * from interim_division
    UNION ALL
    SELECT * from interim_department
    UNION ALL
    SELECT * from interim_project
)

SELECT 
    {{ dbt_utils.generate_surrogate_key(['company_id', 'division_id', 'department_id', 'project_number', 'period_date', 'amount_type', 'org_hierarchy_level']) }} AS sk_id,
    * 
from combined_data
