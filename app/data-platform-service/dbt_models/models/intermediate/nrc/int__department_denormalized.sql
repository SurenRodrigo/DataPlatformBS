WITH department AS (
    SELECT *
    FROM {{ ref('department_seed') }}
),

division AS (
    SELECT *
    FROM {{ ref('division_seed') }}
),

company AS (
    SELECT *
    FROM {{ ref('company_seed') }}
),

tenant AS (
    SELECT *
    FROM {{ ref('tenant_seed') }}
),

final AS (
    SELECT
        department.id                       AS department_id,
        department.organizational_unit_name AS department_name,
        division.id                         AS division_id,
        division.legal_entity_name          AS division_name,
        company.id                          AS company_id,
        company.company_name,
        tenant.id                           AS tenant_id,
        tenant.tenant_name
    FROM department
    LEFT JOIN division
        ON department.legal_entity_id = division.id
    LEFT JOIN company
        ON division.company_id = company.id
    LEFT JOIN tenant
        ON company.tenant_id = tenant.id
)

SELECT *
FROM final
