WITH case_data AS (
    SELECT * FROM {{ ref('stg__endringer_case') }}
),

endringer_project AS (
    SELECT * FROM {{ ref('stg__endringer_project') }}
),

project AS (
    SELECT
        project.id,
        project.tenant_id,
        project.company_id,
        project.company_name,
        project.division_id,
        project.division_name,
        project.department_id,
        project.department_name,
        project.project_identifier,
        endringer_project.project_name,
        endringer_project.project_id                                            AS endringer_project_id,
        endringer_project.project_number
    FROM endringer_project
    LEFT JOIN {{ ref('project') }} AS project
        ON project.project_number = endringer_project.project_number
        
)

SELECT 
    {{ dbt_utils.generate_surrogate_key(['project.id', 'case_data.case_id']) }} AS project_variation_sk,
    project.tenant_id,
    project.company_id,
    project.company_name,
    project.division_id,
    project.division_name,
    project.department_id,
    project.department_name,
    project.id                                                                  AS project_id,
    project.project_number,
    project.project_identifier,
    case_data.case_id                                                           AS ext_variation_id,
    case_data.name,
    case_data.description,
    case_data.variation_date,
    case_data.year,
    case_data.month_number,
    case_data.created_month_name,
    case_data.deadline,
    case_data.date_closed,
    case_data.variation_type_name,
    case_data.variation_type_id,
    case_data.employee_id,
    case_data.status,
    case_data.sender,
    case_data.receiver,
    case_data.created_by,
    case_data.total_amount,
    case_data.accepted_amount,
    case_data.declined_amount,
    case_data.not_handled_amount
FROM case_data
LEFT JOIN project
    ON case_data.ext_project_id = project.endringer_project_id