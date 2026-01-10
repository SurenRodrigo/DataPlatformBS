WITH company_mapping AS (
    SELECT
        company_data_source.company_guid,
        company_data_source.ext_company_id,
        company.company_name,
        company.tenant_id,
        tenant.tenant_name
    FROM {{ ref('data_source_seed') }} AS data_source
    LEFT JOIN {{ ref('company_data_source_seed') }} AS company_data_source
        ON data_source.id = company_data_source.data_source_id
    LEFT JOIN {{ ref('company_seed') }} AS company
        ON company_data_source.company_guid = company.id
    LEFT JOIN {{ ref('tenant_seed') }} AS tenant
        ON company.tenant_id = tenant.id
    WHERE data_source.name = 'Ditio'
),

nrcg_projects AS (
    SELECT
        project_guid,
        project_name,
        project_number,
        company_id,
        company_name,
        external_number
    FROM {{ ref('stg__ditio_nrcg_project') }}
    WHERE company_name != 'NRC KEPT AS'
),

kept_projects AS (
    SELECT
        project_guid,
        project_name,
        project_number,
        company_id,
        company_name,
        external_number
    FROM {{ ref('stg__ditio_kept_project') }}
),

combined_projects AS (
    SELECT * FROM nrcg_projects
    UNION ALL
    SELECT * FROM kept_projects
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['project_number', 'company_mapping.company_guid']) }} AS id,
    company_mapping.tenant_id,
    company_mapping.tenant_name,
    company_mapping.company_guid AS company_id,
    combined_projects.company_id AS ext_company_id,
    company_mapping.company_name,
    project_number AS ext_project_id,
    project_name,
    project_number,
    project_number || ' - ' || project_name AS project_identifier,
    combined_projects.project_guid AS ext_project_guid,
    external_number
FROM combined_projects AS combined_projects
LEFT JOIN company_mapping AS company_mapping
    ON combined_projects.company_id::TEXT = company_mapping.ext_company_id 
    