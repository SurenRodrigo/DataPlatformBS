WITH companies AS (
    SELECT
        company_name,
        id AS company_id
    FROM {{ ref('company_seed') }}
),

divisions AS (
    SELECT
        legal_entity_name AS division_name,
        id                AS division_id,
        company_id
    FROM {{ ref('division_seed') }}
),

departments AS (
    SELECT
        department_name,
        id          AS department_id,
        external_id AS ext_department_id,
        division_id
    FROM {{ ref('int__department_external_id_mapping') }}
),

projects AS (
    SELECT
        project_name,
        project_number,
        project_id,
        organizational_unit_id
    FROM {{ ref('dim_gunnar_knutsen_admmit_project') }}
)

SELECT
    company.company_name,
    company.company_id,
    division.division_name,
    division.division_id,
    department.department_name,
    department.department_id,
    department.ext_department_id,
    project.project_name,
    project.project_number,
    project.project_id
FROM companies AS company
LEFT JOIN divisions AS division ON company.company_id = division.company_id
LEFT JOIN departments AS department ON division.division_id = department.division_id
LEFT JOIN
    projects AS project
    ON department.ext_department_id = project.organizational_unit_id
WHERE company.company_id = 3
