SELECT
    orderid             AS order_id,
    clientid            AS client_id,
    createdon           AS created_on,
    modifiedon          AS modified_on,
    isdeleted           AS is_deleted,
    internalid          AS internal_id,
    itemvehicletypeid   AS item_vehicle_type_id,
    itemvehicletypename AS item_vehicle_type_name,
    dbt_scd_id,
    dbt_updated_at,
    dbt_valid_from,
    dbt_valid_to
FROM {{ ref('admmit_order_item_vehicle_type_snapshot') }}
WHERE dbt_valid_to IS NULL
      AND isdeleted IS false
