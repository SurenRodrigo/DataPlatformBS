WITH variations AS (
    SELECT
        project_variation.variation_type_name,
        project_variation.total_amount,
        company.company_name                AS company,
        division.legal_entity_name          AS division,
        department.organizational_unit_name AS department,
        project_variation.project_id,
        project.project_identifier          AS project,
        project_variation.year,
        project_variation.month_number,
        project_variation.created_month_name,
        project_variation.status,
        project_variation.variation_date
    FROM {{ ref('project_variation') }} AS project_variation
    LEFT JOIN
        {{ ref('company_seed') }} AS company
        ON project_variation.company_id = company.id
    LEFT JOIN
        {{ ref('division_seed') }} AS division
        ON project_variation.division_id = division.id
    LEFT JOIN
        {{ ref('department_seed') }} AS department
        ON project_variation.department_id::integer = department.id
    LEFT JOIN {{ ref('project') }} AS project ON project_variation.project_id = project.id
)

SELECT
    year,
    created_month_name  AS month,
    month_number,
    company,
    division,
    department,
    project_id,
    project,
    variation_type_name,
    status,
    SUM(total_amount)   AS total_amount_sum,
    MIN(variation_date) AS variation_date
FROM variations
GROUP BY
    year,
    month_number,
    created_month_name,
    company,
    division,
    department,
    project_id,
    project,
    variation_type_name,
    status
ORDER BY
    year,
    month_number,
    company,
    division,
    department,
    project_id,
    variation_type_name,
    status
