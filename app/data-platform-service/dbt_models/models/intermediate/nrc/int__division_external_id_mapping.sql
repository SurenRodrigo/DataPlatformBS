WITH division_data_source AS (

    SELECT *
    FROM {{ ref('division_data_source_seed') }}

),


data_source AS (

    SELECT *
    FROM {{ ref('data_source_seed') }}

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
        div_data_source.id,
        div_data_source.tenant_id,
        tenant.tenant_name,
        div_data_source.data_source_id,
        data_source.name                AS data_source_name,
        div_data_source.division_id,
        division.legal_entity_name      AS division_name,
        division.legal_entity_guid      AS division_guid,
        division.company_id             AS company_id,
        company.company_name            AS company_name,
        div_data_source.ext_division_id AS external_id
    FROM division_data_source AS div_data_source
    LEFT JOIN data_source
        ON div_data_source.data_source_id = data_source.id
    LEFT JOIN division
        ON div_data_source.division_id = division.id
    LEFT JOIN company
        ON division.company_id = company.id
    LEFT JOIN tenant
        ON div_data_source.tenant_id = tenant.id

)


SELECT *
FROM final
