WITH amount_data AS (
    SELECT * from {{ ref('stg__endringer_amount')}}
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
    {{ dbt_utils.generate_surrogate_key(['amount_data.ext_project_id', 'amount_data.year', 'amount_data.month_number', 'amount_data.variation_type_name']) }} AS variation_order_sk,
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
    amount_data.year,
    amount_data.month_number,
    amount_data.month_name,
    amount_data.variation_type_name,
    amount_data.total_amount,
    amount_data.total_accepted_amount,
    amount_data.total_declined_amount,
    amount_data.total_not_handled_amount
FROM amount_data
LEFT JOIN project
    ON amount_data.ext_project_id = project.endringer_project_id