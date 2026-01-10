WITH variations AS (
    SELECT
        pv.variation_type_name,
        pv.accepted_amount,
        c.company_name                AS company,
        d.legal_entity_name           AS division,
        dept.organizational_unit_name AS department,
        pv.project_id,
        p.project_identifier          AS project,
        pv.year,
        pv.month_number,
        pv.created_month_name,
        pv.status
    FROM {{ ref('project_variation') }} AS pv
    LEFT JOIN {{ ref('company_seed') }} AS c ON pv.company_id = c.id
    LEFT JOIN {{ ref('division_seed') }} AS d ON pv.division_id = d.id
    LEFT JOIN {{ ref('department_seed') }} AS dept ON pv.department_id::integer = dept.id
    LEFT JOIN {{ ref('project') }} AS p ON pv.project_id = p.id
)

SELECT
    year,
    created_month_name   AS month,
    month_number,
    company,
    division,
    department,
    project_id,
    project,
    variation_type_name,
    status,
    SUM(accepted_amount) AS accepted_amount_sum
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
    year ASC,
    month_number ASC,
    company ASC,
    division ASC,
    department ASC,
    project_id ASC,
    variation_type_name ASC,
    status ASC
