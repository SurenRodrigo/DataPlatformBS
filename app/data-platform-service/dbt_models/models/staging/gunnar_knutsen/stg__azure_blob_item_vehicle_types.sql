SELECT
    "Id"              AS id,
    "Name"            AS name,
    "ClientId"        AS client_id,
    "CreatedOn"       AS created_on,
    "CreatedBy"       AS created_by,
    "ModifiedOn"      AS modified_on,
    "ModifiedBy"      AS modified_by,
    "IsDeleted"       AS is_deleted,
    "SortOrder"       AS sort_order,
    "HideInWorkorder" AS hide_in_workorder,
    dbt_scd_id,
    dbt_updated_at,
    dbt_valid_from,
    dbt_valid_to
FROM {{ ref('gk_azure_blob_item_vehicle_types_snapshot') }}
WHERE dbt_valid_to IS NULL
