WITH project_locations AS (
    SELECT DISTINCT ON (project_id)
        project_id,
        project_location_id
    FROM {{ ref('dim_gunnar_knutsen_admmit_project_locations') }}
    ORDER BY project_id, project_location_id
),

load_unload_place_data AS (
    SELECT
        name,
        internal_id
    FROM {{ ref('stg__admmit_load_unload_place') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['work_order_items.internal_id']) }}
                        AS customer_work_order_line_id,
    2                   AS tenant_id,
    'Gunnar Knutsen'    AS tenant_name,
    6                   AS data_source_id,
    'Admmit'            AS data_source_name,
    work_order_items.project_id      AS project_task_id,
    work_order_items.project_name      AS project_name,
    CASE 
        WHEN work_order_items.project_name IS NULL THEN NULL
        WHEN REGEXP_REPLACE(SPLIT_PART(TRIM(work_order_items.project_name), ' ', 1), '[^0-9]', '', 'g') ~ '^\d+$'
        THEN CAST(REGEXP_REPLACE(SPLIT_PART(TRIM(work_order_items.project_name), ' ', 1), '[^0-9]', '', 'g') AS INTEGER) 
        ELSE NULL
    END AS internal_project_id,
    work_order_items.work_order_id   AS customer_work_order_id,
    NULL                AS loader_employee_id,
    NULL                AS dumper_employee_id,
    work_order_items.load_place_id   AS load_resource_id,
    work_order_items.unload_place_id AS dumper_resource_id,
    work_order_items.employee_id,
    project_locations.project_location_id,
    work_order_items.vehicle_registration_number,
    work_order_items.date,
    work_order_items.invoice_id,
    work_order_items.description,
    work_order_items.number,
    work_order_items.load_place_id,
    load_place.name     AS load_place_name,
    work_order_items.load_place_free_text,
    work_order_items.load_receipt_no,
    work_order_items.unload_place_free_text,
    work_order_items.unload_place_id,
    work_order_items.unload_receipt_no,
    unload_place.name   AS unload_place_name,
    work_order_items.km_start,
    work_order_items.km_stop,
    work_order_items.km_total,
    work_order_items.time_start,
    work_order_items.time_stop,
    work_order_items.time_total,
    work_order_items.time_break,
    work_order_items.comment,
    work_order_items.is_break,
    work_order_items.account_id,
    work_order_items.item_id,
    work_order_items.item_name,
    work_order_items.quantity,
    work_order_items.unit_name,
    work_order_items.price,
    work_order_items.total,
    work_order_items.discount,
    work_order_items.non_invoicable,
    work_order_items.non_settleable,
    work_order_items.weighing_id,
    work_order_items.approved_by_leader,
    work_order_items.is_expense,
    work_order_items.weighing_no,
    work_order_items.est_price,
    work_order_items.no_price,
    work_order_items.est_cost,
    work_order_items.est_expense,
    work_order_items.invoice_no,
    work_order_items.invoice_status_id,
    work_order_items.order_id,
    work_order_items.order_type,
    work_order_items.order_no,
    work_order_items.customer_order_no,
    work_order_items.our_ref,
    work_order_items.your_ref,
    work_order_items.driver_description,
    work_order_items.time_from,
    work_order_items.time_to,
    work_order_items.receipt_address,
    work_order_items.customer_signed,
    work_order_items.responsible_employee_signed,
    work_order_items.responsible_employee_signed_id,
    work_order_items.responsible_employee_signed_name,
    work_order_items.agreed_price,
    work_order_items.employee_finalized,
    work_order_items.created_on,
    work_order_items.modified_on,
    work_order_items.is_deleted
FROM {{ ref('stg__admmit_work_order_item') }} AS work_order_items
LEFT JOIN project_locations AS project_locations
    ON work_order_items.project_id = project_locations.project_id
LEFT JOIN load_unload_place_data AS load_place
    ON work_order_items.load_place_id = load_place.internal_id
LEFT JOIN load_unload_place_data AS unload_place
    ON work_order_items.unload_place_id = unload_place.internal_id
