SELECT
    tenant.id  AS tenant_id,
    project.id AS project_id,
    project.data_source_name,
    project.ext_project_id,
    project.company_name
    -- commented since project table doesnt contain is_active attribute
    -- NULL AS is_active
FROM {{ ref('project') }} AS project
LEFT JOIN {{ ref('tenant_seed') }} AS tenant ON tenant.id = 1 -- NRC Group AS

UNION ALL

SELECT
    company.tenant_id,
    project.project_id     AS project_id,
    'Endringer'            AS data_source_name,
    project.project_number AS ext_project_id,
    company.company_name
FROM {{ ref('stg__endringer_project') }} AS project
LEFT JOIN {{ ref('company_data_source_seed') }} AS ext_company ON ext_company.ext_company_id = project.ext_organization_id
LEFT JOIN {{ ref('company_seed')}} AS company ON company.id = ext_company.company_guid