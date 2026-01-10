SELECT
    id                                        AS project_guid,
    NULLIF(name, '')                          AS project_name,
    NULLIF(number, '')::INT                   AS project_number,
    NULLIF(companyid, '')                     AS company_id,
    NULLIF(companyname, '')                   AS company_name,
    companydata                               AS company_data,
    NULLIF(createdby, '')                     AS created_by,
    NULLIF(modifiedby, '')                    AS modified_by,
    isdeleted                                 AS is_deleted,
    isexternal                                AS is_external,
    NULLIF(externaldim01, '')                 AS external_dim_01,
    NULLIF(externalnumber, '')                AS external_number,
    NULLIF(createddatetime, '')::TIMESTAMP    AS created_at,
    NULLIF(modifieddatetime, '')::TIMESTAMP   AS modified_at,
    dbt_scd_id,
    dbt_updated_at,
    dbt_valid_from,
    dbt_valid_to
FROM {{ ref('ditio_kept_project_snapshot') }}
WHERE dbt_valid_to IS NULL 