SELECT
    internal_id,
    client_id,
    project_id,
    hanger_name,
    hanger_id,
    hanger_registration_number
FROM {{ ref('stg__admmit_work_order_item') }}    
GROUP BY internal_id, client_id, project_id, hanger_name, hanger_id, hanger_registration_number