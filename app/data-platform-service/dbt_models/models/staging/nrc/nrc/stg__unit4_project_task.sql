SELECT
    task_identifier,
    companyid::INTEGER                          AS company_id,
    NULLIF(attributeid, '')                     AS attribute_id,
    NULLIF(attributevalue, '')::TEXT            AS attribute_value,
    NULLIF(attributename, '')                   AS attribute_name,
    NULLIF(owner, '')                           AS owner,
    NULLIF(status, '')                          AS status,
    NULLIF(description, '')                     AS description,
    customvalue                                 AS custom_value,
    periodfrom                                  AS period_from,
    periodto                                    AS period_to,
    NULLIF(ownerattributeid, '')                AS owner_attribute_id,
    NULLIF(ownerattributename, '')              AS owner_attribute_name,
    (lastupdated ->> 'updatedAt')::TIMESTAMP    AS last_updated_at,
    dbt_scd_id,
    dbt_updated_at,
    dbt_valid_from,
    dbt_valid_to
FROM {{ ref('unit4_project_task_snapshot') }}
WHERE dbt_valid_to IS NULL
