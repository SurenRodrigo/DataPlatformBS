WITH department_data_source AS (
    SELECT *
    FROM {{ ref('department_data_source_seed') }}
),


data_source AS (

    SELECT *
    FROM {{ ref('data_source_seed') }}

),


department AS (
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
        dep_data_source.id,
        dep_data_source.organization_unit_id AS department_id,
        department.organizational_unit_name  AS department_name,
        division.id                          AS division_id,
        division.legal_entity_name           AS division_name,
        company.id                           AS company_id,
        company.company_name,
        dep_data_source.tenant_id,
        tenant.tenant_name,
        dep_data_source.data_source_id,
        data_source.name                     AS data_source_name,
        dep_data_source.ext_department_id    AS external_id
    FROM department_data_source AS dep_data_source
    LEFT JOIN data_source
        ON dep_data_source.data_source_id = data_source.id
    LEFT JOIN department
        ON dep_data_source.organization_unit_id = department.id
    LEFT JOIN division
        ON department.legal_entity_id = division.id
    LEFT JOIN company
        ON division.company_id = company.id
    LEFT JOIN tenant
        ON dep_data_source.tenant_id = tenant.id
)

SELECT *
FROM final
