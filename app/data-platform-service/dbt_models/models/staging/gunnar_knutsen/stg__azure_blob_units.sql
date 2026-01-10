SELECT
    "Id"                          AS id,
    "Name"                        AS name,
    "Variants"                    AS variants,
    "IsDeleted"                   AS is_deleted,
    "CreatedBy"                   AS created_by,
    "CreatedOn"                   AS created_on,
    "ModifiedBy"                  AS modified_by,
    "ModifiedOn"                  AS modified_on,
    "WoQtyFromHours"               AS wo_qty_from_hours,
    dbt_scd_id,
    dbt_updated_at,
    dbt_valid_from,
    dbt_valid_to
FROM {{ ref('gk_azure_blob_units_snapshot') }}
WHERE dbt_valid_to IS NULL AND "IsDeleted" = FALSE