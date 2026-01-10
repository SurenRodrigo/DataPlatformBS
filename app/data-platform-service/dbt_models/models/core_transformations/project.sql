-- commented fields which does not has a source property
SELECT
    {{ dbt_utils.generate_surrogate_key(['int_dept_map.company_id','project.project_id']) }} AS id,
    int_dept_map.tenant_id           AS tenant_id,    
    int_dept_map.company_id          AS company_id,
    int_dept_map.company_name        AS company_name,
    int_dept_map.division_id         AS division_id,
    int_dept_map.division_name       AS division_name,
    int_dept_map.department_id       AS department_id,
    int_dept_map.department_name     AS department_name,
    customer.customer_id,
    employee.id                      AS project_manager_id, 
    project_id                       AS ext_project_id,
    project_name,
    project_id                       AS project_number,
    project_id || ' - ' || project_name                       AS project_identifier,
    project_type,
    status                                                    AS project_status,
    CAST(project_duration ->> 'dateTo' AS TIMESTAMP)::DATE    AS project_finished_date_planned,
    CAST(project_duration ->> 'dateFrom' AS TIMESTAMP)::DATE  AS project_start_date,
    CAST(project_duration ->> 'completed' AS TIMESTAMP)::DATE AS project_finished_date,
    -- NULL                             AS planned_start_date,
    -- NULL                             AS project_address,
    -- NULL                             AS project_location_latitude,
    -- NULL                             AS is_active,
    last_updated_at,
    last_updated_by,
    int_dept_map.data_source_name     AS data_source_name
FROM {{ ref('stg__unit4_project') }} AS project
LEFT JOIN {{ ref('customer') }} AS customer 
    ON (project.customer_information->>'customerId') = customer.ext_customer_id
LEFT JOIN {{ ref('employee') }} AS employee 
    ON project.project_manager_id = employee.employee_id
LEFT JOIN {{ ref('int__department_external_id_mapping') }} AS int_dept_map 
    ON project.department_id = int_dept_map.external_id and int_dept_map.data_source_id = 7 -- Unit4
