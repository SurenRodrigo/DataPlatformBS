WITH unit4_project_tasks AS (
    SELECT
        project_task_sk,
        tenant_id,
        tenant_name,
        project_id,
        ext_project_task_id,
        ext_production_code,
        project_task_name       AS task_name,
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
    FROM {{ ref('int__nrc_project_task') }}
)

SELECT
    pt.project_task_sk,
    pt.tenant_id,
    pt.tenant_name,
    pt.project_id,
    pt.ext_project_task_id,
    pt.ext_production_code,
    pt.task_name,
    pt.project_task_identifier,
    pt.project_name,
    pt.status,
    pt.company_id,
    pt.company_name,
    pt.division_id,
    pt.division_name,
    pt.department_id,
    pt.department_name,
    pt.data_source_id,
    pt.data_source_name
FROM unit4_project_tasks AS pt