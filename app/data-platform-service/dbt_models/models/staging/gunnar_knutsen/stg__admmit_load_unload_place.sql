SELECT
    clientid            AS client_id,
    customerid          AS customer_id,
    externalid          AS external_id,
    internalid          AS internal_id,
    externalownerid     AS external_owner_id,
    load                AS load,
    name                AS name,
    unload              AS unload,
    receipt             AS receipt,
    createdon           AS created_on,
    isdeleted           AS is_deleted,
    modifiedon          AS modified_on,
    description         AS description,
    receiptinfo         AS receipt_info,
    externalsystemname  AS external_system_name,
    ignoreprojectfilter AS ignore_project_filter,
    dbt_scd_id,
    dbt_updated_at,
    dbt_valid_from,
    dbt_valid_to
FROM {{ ref('admmit_load_unload_snapshot') }}
WHERE dbt_valid_to IS NULL
