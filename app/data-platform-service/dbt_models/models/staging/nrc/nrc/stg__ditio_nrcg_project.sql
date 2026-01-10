SELECT
    id                                        AS project_guid,
    NULLIF(name, '')                          AS project_name,
    NULLIF(number, '')::INT                   AS project_number,
    NULLIF(companyid, '')                     AS company_id,
    NULLIF(createdby, '')                     AS created_by,
    NULLIF(deletedby, '')                     AS deleted_by,
    isdeleted                                 AS is_deleted,
    isexternal                                AS is_external,
    NULLIF(modifiedby, '')                    AS modified_by,
    companydata                               AS company_data,
    NULLIF(companyname, '')                   AS company_name,
    NULLIF(externaldim01, '')                 AS external_dim_01,
    NULLIF(externaldim02, '')                 AS external_dim_02,
    NULLIF(externalnumber, '')                AS external_number,
    NULLIF(createddatetime, '')::TIMESTAMP    AS created_at,
    NULLIF(deleteddatetime, '')::TIMESTAMP    AS deleted_at,
    NULLIF(modifieddatetime, '')::TIMESTAMP   AS modified_at,
    -- DBT metadata
    dbt_scd_id,
    dbt_updated_at,
    dbt_valid_from,
    dbt_valid_to
FROM {{ ref('ditio_nrcg_project_snapshot') }}
WHERE dbt_valid_to IS NULL
  AND number ~ '^[0-9]+$'
