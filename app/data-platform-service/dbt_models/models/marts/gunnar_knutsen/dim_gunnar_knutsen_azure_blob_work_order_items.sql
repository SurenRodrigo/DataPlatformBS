WITH work_order_items AS (
    SELECT *
    FROM {{ ref('stg__azure_blob_work_order_items') }}
),

items AS (
    SELECT
        id,
        item_name,
        unit_id
    FROM {{ ref('stg__azure_blob_items') }}
),

work_orders AS (
    SELECT
        id,
        project_id,
        order_no,
        order_id,
        work_order_date,
        customer_id
    FROM {{ ref('stg__azure_blob_work_orders') }}
),

projects AS (
    SELECT
        id,
        project_number
    FROM {{ ref('stg__azure_blob_projects') }}
),

vehicles AS (
    SELECT
        id,
        registration_number,
        fuel_type
    FROM {{ ref('stg__azure_blob_vehicles') }}
),

item_vehicle_types AS (
    SELECT
        id,
        name
    FROM {{ ref('stg__azure_blob_item_vehicle_types') }}
),

load_unload_places AS (
    SELECT
        id,
        name
    FROM {{ ref('stg__azure_blob_load_unload_place') }}
),

units AS (
    SELECT
        id,
        name
    FROM {{ ref('stg__azure_blob_units') }}
),

customers AS (
    SELECT
        id,
        name
    FROM {{ ref('stg__azure_blob_customer') }}
)

SELECT
    work_order_items.id,
    work_order_items.work_order_id,
    work_order_items.number,
    work_order_items.vehicle_id,
    work_order_items.hanger_id,
    work_order_items.employee_id,
    work_order_items.load_place_id,
    work_order_items.load_receipt_no,
    work_order_items.load_invoiced,
    work_order_items.unload_place_id,
    work_order_items.unload_receipt_no,
    work_order_items.km_start,
    work_order_items.km_stop,
    work_order_items.km_total,
    work_order_items.time_start,
    work_order_items.time_stop,
    work_order_items.time_total,
    work_order_items.time_break,
    work_order_items.client_id,
    work_order_items.created_on,
    work_order_items.created_by,
    work_order_items.modified_on,
    work_order_items.modified_by,
    work_order_items.is_deleted,
    work_order_items.comment,
    work_order_items.is_break,
    work_order_items.vat_id,
    work_order_items.account_id,
    work_order_items.item_id,
    work_order_items.price,
    work_order_items.discount,
    work_order_items.total,
    work_order_items.vehicle_owner_id,
    work_order_items.vehicle_owner_percentage,
    work_order_items.vehicle_owner_total,
    work_order_items.vehicle_owner_comment,
    work_order_items.quantity,
    work_order_items.non_invoicable,
    work_order_items.non_settleable,
    work_order_items.weighing_id,
    work_order_items.load_place_free_text,
    work_order_items.unload_place_free_text,
    work_order_items.item_vehicle_type_id,
    work_order_items.approved_date_leader,
    work_order_items.approved_by_leader,
    work_order_items.is_expense,
    work_order_items.weighing_no,
    work_order_items.est_price,
    work_order_items.est_settle,
    work_order_items.no_price,
    work_order_items.est_cost,
    work_order_items.est_expense,
    work_order_items.text1,
    work_order_items.text2,
    items.item_name,
    projects.project_number,
    vehicles.registration_number as vehicle_registration_number,
    vehicles.fuel_type,
    work_orders.order_no,
    work_orders.order_id,
    work_orders.work_order_date,
    item_vehicle_types.name as item_vehicle_type_name,
    load_place.name as load_place_name,
    unload_place.name as unload_place_name,
    units.name as unit_name,
    customers.name as customer_name
FROM work_order_items
LEFT JOIN items
    ON work_order_items.item_id = items.id
LEFT JOIN work_orders
    ON work_order_items.work_order_id = work_orders.id
LEFT JOIN projects
    ON work_orders.project_id = projects.id
LEFT JOIN vehicles
    ON work_order_items.vehicle_id = vehicles.id
LEFT JOIN item_vehicle_types
    ON work_order_items.item_vehicle_type_id = item_vehicle_types.id
LEFT JOIN load_unload_places AS load_place
    ON work_order_items.load_place_id = load_place.id
LEFT JOIN load_unload_places AS unload_place
    ON work_order_items.unload_place_id = unload_place.id
LEFT JOIN units
    ON items.unit_id = units.id
LEFT JOIN customers
    ON work_orders.customer_id = customers.id