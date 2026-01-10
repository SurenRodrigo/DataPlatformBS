WITH project AS (
    SELECT
        project_name,
        ext_project_id AS project_number,
        company_id,
        CASE
            WHEN project_status = 'N' THEN true
            ELSE false
        END AS is_active,
        last_updated_at
    FROM {{ ref('project')}}
),
company_data_source_seed AS (
    SELECT
        company_guid,
        data_source_id,
        ext_company_id as organization_id
    FROM {{ ref('company_data_source_seed_public') }}
    WHERE data_source_id = 5
),
project_data_source AS (
    SELECT
        ext_project_id,
        project_id
    FROM {{ ref('project_data_source') }}
    WHERE data_source_name = 'Endringer'
)
SELECT 
    project.project_name,
    project.project_number,
    project_data_source.project_id,
    company.organization_id,
    project.is_active,
    project.last_updated_at
FROM project
LEFT JOIN company_data_source_seed AS company ON company.company_guid = project.company_id
LEFT JOIN project_data_source ON project_data_source.ext_project_id = project.project_number