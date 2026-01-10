SELECT
    company,
    department,
    organization_number,
    internal_place_id,
    item_number,
    item_name_loading,
    item_name_unloading,
    price,
    unit,
    internal_item_id,
    item_name_admmit,
    admmit_alternative
FROM {{ ref('stg_admmit_compello_data') }}
