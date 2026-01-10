WITH base_data AS (
    SELECT
        project_task.company_id,
        project_task.attribute_value AS ext_project_task_id,
        project_task.description AS project_task_name,
        project_task.status
    FROM {{ ref('stg__unit4_project_task') }} AS project_task
),

company_mapped AS (
    SELECT
        base_data.company_id,
        base_data.ext_project_task_id,
        base_data.project_task_name,
        base_data.status,
        company_mapping.company_guid AS mapped_company_guid
    FROM base_data
    LEFT JOIN {{ ref('company_data_source_seed') }} AS company_mapping
        ON base_data.company_id::TEXT = company_mapping.ext_company_id
        AND company_mapping.data_source_id = 7
),

project_parsing AS (
    SELECT
        company_mapped.company_id,
        company_mapped.ext_project_task_id,
        company_mapped.project_task_name,
        company_mapped.status,
        company_mapped.mapped_company_guid,
        -- Finding the project ID 
        COALESCE(
            CASE WHEN LENGTH(company_mapped.ext_project_task_id) >= 6 
                 AND EXISTS (SELECT 1 FROM {{ ref('project') }} AS project 
                           WHERE LEFT(company_mapped.ext_project_task_id, 6)::INT = project.ext_project_id 
                           AND company_mapped.mapped_company_guid = project.company_id)
                 THEN LEFT(company_mapped.ext_project_task_id, 6) END,
            
            CASE WHEN LENGTH(company_mapped.ext_project_task_id) >= 5 
                 AND EXISTS (SELECT 1 FROM {{ ref('project') }} AS project 
                           WHERE LEFT(company_mapped.ext_project_task_id, 5)::INT = project.ext_project_id 
                           AND company_mapped.mapped_company_guid = project.company_id)
                 THEN LEFT(company_mapped.ext_project_task_id, 5) END,

            CASE WHEN LENGTH(company_mapped.ext_project_task_id) >= 4 
                 AND EXISTS (SELECT 1 FROM {{ ref('project') }} AS project 
                           WHERE LEFT(company_mapped.ext_project_task_id, 4)::INT = project.ext_project_id 
                           AND company_mapped.mapped_company_guid = project.company_id)
                 THEN LEFT(company_mapped.ext_project_task_id, 4) END,

            CASE WHEN LENGTH(company_mapped.ext_project_task_id) >= 3 
                 AND EXISTS (SELECT 1 FROM {{ ref('project') }} AS project 
                           WHERE LEFT(company_mapped.ext_project_task_id, 3)::INT = project.ext_project_id 
                           AND company_mapped.mapped_company_guid = project.company_id)
                 THEN LEFT(company_mapped.ext_project_task_id, 3) END,

            CASE WHEN LENGTH(company_mapped.ext_project_task_id) >= 2 
                 AND EXISTS (SELECT 1 FROM {{ ref('project') }} AS project 
                           WHERE LEFT(company_mapped.ext_project_task_id, 2)::INT = project.ext_project_id 
                           AND company_mapped.mapped_company_guid = project.company_id)
                 THEN LEFT(company_mapped.ext_project_task_id, 2) END
        ) AS ext_project_id
    FROM company_mapped
),

project_details AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['project_parsing.ext_project_id', 'project_parsing.ext_project_task_id', 'project_parsing.project_task_name']) }} AS project_task_sk,
        department_mapping.tenant_id,
        department_mapping.tenant_name,
        project_parsing.ext_project_id::INT AS project_id,
        project_parsing.ext_project_task_id,
        CASE
            WHEN project_parsing.ext_project_task_id LIKE project_parsing.ext_project_id || '%'
            THEN NULLIF(
                RIGHT(
                    project_parsing.ext_project_task_id,
                    LENGTH(project_parsing.ext_project_task_id)
                    - LENGTH(project_parsing.ext_project_id)
                ),
                ''
                )
            ELSE project_parsing.ext_project_task_id
        END AS ext_production_code,
        project_parsing.project_task_name,
        project_parsing.status,
        project.project_name,
        department_mapping.company_id,
        department_mapping.company_name,
        department_mapping.division_id,
        department_mapping.division_name,
        department_mapping.department_id,
        department_mapping.department_name,
        department_mapping.data_source_id,
        department_mapping.data_source_name,
        project_parsing.ext_project_task_id || ' - ' || project_parsing.project_task_name AS project_task_identifier
    FROM project_parsing
    LEFT JOIN {{ ref('project') }} AS project
        ON project_parsing.ext_project_id::INT = project.ext_project_id
        AND project_parsing.mapped_company_guid = project.company_id
    LEFT JOIN {{ ref('int__department_external_id_mapping') }} AS department_mapping
        ON project.department_id = department_mapping.department_id
        AND department_mapping.data_source_name = 'Unit4'
)

SELECT
    project_task_sk,
    tenant_id,
    tenant_name,
    project_id,
    ext_project_task_id,
    ext_production_code,
    project_task_name,
    status,
    project_task_identifier,
    project_name,
    company_id,
    company_name,
    division_id,
    division_name,
    department_id,
    department_name,
    data_source_id,
    data_source_name
FROM project_details