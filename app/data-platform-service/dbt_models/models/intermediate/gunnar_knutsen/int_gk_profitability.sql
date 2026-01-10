WITH fuel_types AS (
    SELECT
        "Reg.no" AS reg_no,
        "Fuel"   AS fuel_type
    FROM {{ ref('fuel_type_seed') }}
),

work_order_items AS (
    SELECT *
    FROM {{ ref('stg__admmit_work_order_item') }}
),

vehicles AS (
    SELECT
        vehicle_type_id,
        vehicle_type_name,
        internal_id
    FROM {{ref('dim_gunnar_knutsen_admmit_vehicle')}}
),

load_unload_place_data AS (
    SELECT
        name,
        internal_id
    FROM {{ ref('stg__admmit_load_unload_place') }}
),

admmit_vehicle_types AS (
    SELECT
        item_vehicle_type_id,
        item_vehicle_type_name
    FROM {{ ref('stg__admmit_vehicle_types') }}
)

SELECT
    work_order_items.date,
    work_order_items.txt1,
    work_order_items.txt2,
    work_order_items.txt3,
    work_order_items.phone,
    work_order_items.price,
    work_order_items.total,
    work_order_items.vat_id,
    work_order_items.item_id,
    work_order_items.km_stop,
    work_order_items.number,
    work_order_items.our_ref,
    work_order_items.price1,
    work_order_items.price2,
    work_order_items.price3,
    work_order_items.time_to,
    work_order_items.comment,
    work_order_items.est_cost,
    work_order_items.is_break,
    work_order_items.km_start,
    work_order_items.km_total,
    work_order_items.no_price,
    work_order_items.order_id,
    work_order_items.order_no,
    work_order_items.your_ref,
    work_order_items.client_id,
    work_order_items.discount,
    work_order_items.est_price,
    work_order_items.hanger_id,
    work_order_items.invoiced,
    work_order_items.item_name,
    work_order_items.quantity,
    work_order_items.time_from,
    work_order_items.time_stop,
    work_order_items.unit_name,
    work_order_items.account_id,
    work_order_items.created_on,
    work_order_items.est_settle,
    work_order_items.invoice_id,
    work_order_items.invoice_no,
    work_order_items.is_deleted,
    work_order_items.is_expense,
    work_order_items.order_type,
    work_order_items.project_id,
    work_order_items.time_break,
    work_order_items.time_start,
    work_order_items.time_total,
    work_order_items.vehicle_id,
    work_order_items.customer_id,
    work_order_items.employee_id,
    work_order_items.est_expense,
    work_order_items.hanger_name,
    work_order_items.internal_id,
    work_order_items.modified_on,
    work_order_items.supplier_id,
    work_order_items.weighing_id,
    work_order_items.weighing_no,
    work_order_items.agreed_price,
    work_order_items.contact_info,
    work_order_items.description,
    work_order_items.load_place_id,
    work_order_items.project_name,
    work_order_items.vehicle_name,
    work_order_items.work_order_id,
    work_order_items.customer_name,
    work_order_items.employee_name,
    work_order_items.load_invoiced,
    work_order_items.supplier_name,
    work_order_items.load_receipt_no,
    work_order_items.non_invoicable,
    work_order_items.non_settleable,
    work_order_items.unload_place_id,
    load_place.name     AS load_place_name,
    unload_place.name   AS unload_place_name,
    work_order_items.customer_signed,
    work_order_items.receipt_address,
    work_order_items.supplier_status,
    work_order_items.vehicle_owner_id,
    work_order_items.customer_order_no,
    work_order_items.invoice_status_id,
    work_order_items.unload_receipt_no,
    work_order_items.approved_by_leader,
    work_order_items.foreman_receipt_id,
    work_order_items.vehicle_owner_name,
    work_order_items.customer_receipt_id,
    work_order_items.driver_description,
    work_order_items.employee_finalized,
    work_order_items.item_vehicle_type_id,
    work_order_items.load_place_free_text,
    work_order_items.vehicle_owner_total,
    work_order_items.approved_date_leader,
    work_order_items.customer_location_id,
    work_order_items.message_for_employee,
    work_order_items.supplier_status_date,
    work_order_items.unload_place_free_text,
    work_order_items.vehicle_owner_comment,
    work_order_items.foreman_receipt_content,
    work_order_items.customer_receipt_content,
    work_order_items.vehicle_owner_percentage,
    work_order_items.foreman_receipt_recipient,
    work_order_items.customer_receipt_recipient,
    work_order_items.hanger_registration_number,
    work_order_items.driver_description_customer,
    work_order_items.responsible_employee_signed,
    work_order_items.vehicle_registration_number,
    work_order_items.customer_receipt_attachments,
    work_order_items.supplier_comment_for_customer,
    work_order_items.responsible_employee_signed_id,
    work_order_items.customer_signature_attachment_id,
    work_order_items.responsible_employee_signed_name,
    admmit_vehicle_types.item_vehicle_type_name,
    fuel_types.fuel_type,
    vehicles.vehicle_type_name,
    COALESCE(work_order_items.est_price, 0)
    * COALESCE(work_order_items.quantity, 0) AS total_income,
    COALESCE(work_order_items.est_cost, 0)
    * COALESCE(work_order_items.quantity, 0) AS total_cost,
    COALESCE(work_order_items.est_settle, 0)
    * COALESCE(work_order_items.quantity, 0) AS total_avr,
    COALESCE(work_order_items.est_expense, 0)
    * COALESCE(work_order_items.quantity, 0) AS total_outlay
FROM work_order_items
LEFT JOIN fuel_types
    ON work_order_items.vehicle_registration_number = fuel_types.reg_no
LEFT JOIN vehicles
    ON work_order_items.vehicle_id = vehicles.internal_id
LEFT JOIN load_unload_place_data AS load_place
    ON work_order_items.load_place_id = load_place.internal_id
LEFT JOIN load_unload_place_data AS unload_place
    ON work_order_items.unload_place_id = unload_place.internal_id
LEFT JOIN admmit_vehicle_types
    ON work_order_items.item_vehicle_type_id = admmit_vehicle_types.item_vehicle_type_id

